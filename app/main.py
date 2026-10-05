import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from opentelemetry import metrics, trace, _logs
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter, BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, ConsoleMetricExporter
import sys
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor, ConsoleLogRecordExporter, BatchLogRecordProcessor

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter


class SafeStdoutWriter:
    def write(self, s):
        try:
            sys.stdout.write(s)
            sys.stdout.flush()
        except (ValueError, OSError):
            pass

    def flush(self):
        try:
            sys.stdout.flush()
        except (ValueError, OSError):
            pass


safe_out = SafeStdoutWriter()
resource = Resource.create({"service.name": "order-tracker"})

# Traces
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter(out=safe_out)))

# Metrics
metric_readers = []
console_metric_reader = PeriodicExportingMetricReader(ConsoleMetricExporter(out=safe_out), export_interval_millis=1000)
metric_readers.append(console_metric_reader)

# Logs
logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(SimpleLogRecordProcessor(ConsoleLogRecordExporter(out=safe_out)))
_logs.set_logger_provider(logger_provider)

# OTLP Collector Export if endpoint is configured
otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
if otlp_endpoint:
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)))
    otlp_metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True), export_interval_millis=1000)
    metric_readers.append(otlp_metric_reader)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(endpoint=otlp_endpoint, insecure=True)))

trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer("order-tracker")

meter_provider = MeterProvider(resource=resource, metric_readers=metric_readers)
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("order-tracker")

request_counter = meter.create_counter(
    name="order_lookup_requests_total",
    description="Total count of order lookup requests",
    unit="1",
)
http_request_counter = meter.create_counter(
    name="http_requests_total",
    description="Total HTTP requests",
    unit="1",
)

otel_logging_handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
logger = logging.getLogger("order-tracker")
logger.setLevel(logging.INFO)
logger.addHandler(otel_logging_handler)

DB_PATH = Path(os.getenv("ORDER_DB_PATH", "data/orders.db"))
STATUSES = {"received", "preparing", "shipped", "delivered"}


def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def init_db():
    with connect() as db:
        db.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                customer TEXT NOT NULL,
                item TEXT NOT NULL,
                priority TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            )"""
        )
        if db.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0:
            now = datetime.now(timezone.utc)
            previous_month_end = now.replace(day=1) - timedelta(days=1)
            for order in (
                ("standard-1001", "Avery", "Notebook", "standard", "received", now),
                ("express-1002", "Sam", "Headphones", "express", "preparing", previous_month_end),
                ("standard-1003", "Riley", "Water bottle", "standard", "shipped", now),
            ):
                db.execute(
                    "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
                    (*order[:5], order[5].isoformat()),
                )


def as_dict(row):
    return dict(row) if row else None


def order_detail(row):
    order = as_dict(row)
    if order["priority"] == "express":
        placed_at = datetime.fromisoformat(order["created_at"])
        estimated_at = placed_at + timedelta(days=2)
        order["estimated_delivery"] = estimated_at.date().isoformat()
    return order


class NewOrder(BaseModel):
    customer: str = Field(min_length=1, max_length=80)
    item: str = Field(min_length=1, max_length=120)
    priority: str = "standard"


class StatusUpdate(BaseModel):
    status: str


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Order Tracker", lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent.parent / "static" / "index.html")


@app.get("/healthz")
def health():
    with connect() as db:
        db.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/api/orders")
def list_orders():
    with connect() as db:
        rows = db.execute("SELECT * FROM orders ORDER BY created_at DESC").fetchall()
    return [as_dict(row) for row in rows]


@app.get("/api/orders/{order_id}")
def get_order(order_id: str):
    route = "/api/orders/{order_id}"
    with tracer.start_as_current_span("order_lookup") as span:
        span.set_attribute("route", route)
        span.set_attribute("order.id", order_id)
        with connect() as db:
            row = db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            status_code = 404
            span.set_attribute("http.status_code", status_code)
            span.set_attribute("status_code", status_code)
            attrs = {"route": route, "status_code": status_code, "http.status_code": status_code}
            request_counter.add(1, attrs)
            http_request_counter.add(1, attrs)
            logger.warning(f"Order {order_id} not found", extra=attrs)
            console_metric_reader.force_flush()
            raise HTTPException(404, "Order not found")

        try:
            detail = order_detail(row)
            status_code = 200
            span.set_attribute("http.status_code", status_code)
            span.set_attribute("status_code", status_code)
            attrs = {"route": route, "status_code": status_code, "http.status_code": status_code}
            request_counter.add(1, attrs)
            http_request_counter.add(1, attrs)
            logger.info(f"Order {order_id} retrieved successfully", extra=attrs)
            console_metric_reader.force_flush()
            return detail
        except Exception as e:
            status_code = 500
            span.set_attribute("http.status_code", status_code)
            span.set_attribute("status_code", status_code)
            span.record_exception(e)
            attrs = {"route": route, "status_code": status_code, "http.status_code": status_code}
            request_counter.add(1, attrs)
            http_request_counter.add(1, attrs)
            logger.error(f"Error retrieving order {order_id}: {e}", exc_info=True, extra=attrs)
            console_metric_reader.force_flush()
            raise HTTPException(500, f"Error retrieving order: {e}")


@app.post("/api/orders", status_code=201)
def create_order(order: NewOrder):
    if order.priority not in {"standard", "express"}:
        raise HTTPException(422, "Priority must be standard or express")
    order_id = str(uuid4())
    with connect() as db:
        db.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, order.customer, order.item, order.priority, "received",
             datetime.now(timezone.utc).isoformat()),
        )
    return get_order(order_id)


@app.patch("/api/orders/{order_id}")
def update_status(order_id: str, update: StatusUpdate):
    if update.status not in STATUSES:
        raise HTTPException(422, "Invalid status")
    with connect() as db:
        cursor = db.execute(
            "UPDATE orders SET status = ? WHERE id = ?",
            (update.status, order_id),
        )
    if cursor.rowcount == 0:
        raise HTTPException(404, "Order not found")
    return get_order(order_id)
