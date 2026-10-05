import http.server
import json
import logging
import os
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("incident-responder")

PORT = int(os.getenv("RESPONDER_PORT", "8001"))
INCIDENTS_DIR = Path(__file__).parent / "incidents"
INCIDENTS_DIR.mkdir(parents=True, exist_ok=True)
LATEST_INCIDENT_FILE = Path(__file__).parent / "latest_incident.json"
LATEST_RESPONSE_FILE = Path(__file__).parent / "latest_response.txt"


def fetch_loki_logs():
    try:
        url = "http://localhost:3100/loki/api/v1/query_range?" + urllib.parse.urlencode({
            "query": '{service_name="order-tracker"}',
            "limit": 20
        })
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
            logs = []
            for stream in data.get("data", {}).get("result", []):
                for val in stream.get("values", []):
                    logs.append(f"{val[0]}: {val[1]}")
            return "\n".join(logs)
    except Exception as e:
        logger.warning(f"Could not fetch Loki logs: {e}")
        return ""


def fetch_tempo_traces():
    try:
        url = "http://localhost:3200/api/search?tags=status_code=500&limit=5"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
            return json.dumps(data, indent=2)
    except Exception as e:
        logger.warning(f"Could not fetch Tempo traces: {e}")
        return ""


def run_agent(alert_data, logs, traces, endpoint):
    alerts = alert_data.get("alerts", [alert_data])
    first_alert = alerts[0] if alerts else {}
    labels = first_alert.get("labels", {})
    annotations = first_alert.get("annotations", {})
    summary = annotations.get("summary", "Alert triggered")
    is_test = labels.get("test") == "true" or "no incident to fix" in summary.lower()

    if is_test:
        prompt = (
            f"An alert notification was received:\n"
            f"Alert Name: {labels.get('alertname', 'Unknown')}\n"
            f"Test Flag: {labels.get('test', 'false')}\n"
            f"Summary: {summary}\n\n"
            f"Please review this alert notification and provide your response."
        )
    else:
        repo_root = Path(__file__).parent.parent
        prompt = (
            f"ALERT FIRING in order-tracker!\n"
            f"Summary: {summary}\n"
            f"Affected Endpoint: {endpoint}\n"
            f"Repository Path: {repo_root}\n"
            f"Recent Logs:\n{logs}\n\n"
            f"Recent Traces:\n{traces}\n\n"
            f"An incident has occurred in the Order Tracker service. "
            f"Please investigate the root cause, fix the bug in the order-tracker code if needed, "
            f"and describe what was wrong and how it was resolved."
        )

    logger.info("Starting coding assistant in headless mode via agy...")
    cmd = [
        "agy",
        "--dangerously-skip-permissions",
        "-p",
        prompt
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent),
            timeout=180
        )
        response_text = result.stdout.strip() if result.stdout else result.stderr.strip()
        logger.info(f"Agent finished. Response:\n{response_text}")
        LATEST_RESPONSE_FILE.write_text(response_text)
        return response_text
    except Exception as e:
        logger.error(f"Failed to run agy: {e}")
        err_msg = f"Agent execution failed: {e}"
        LATEST_RESPONSE_FILE.write_text(err_msg)
        return err_msg


class AlertHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/alerts":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode()
            try:
                alert_data = json.loads(body)
            except Exception:
                alert_data = {"raw": body}

            logger.info(f"Received alert at /alerts: {json.dumps(alert_data)}")

            endpoint = ""
            for a in alert_data.get("alerts", []):
                endpoint = a.get("annotations", {}).get("endpoint") or a.get("labels", {}).get("endpoint") or endpoint

            logs = fetch_loki_logs()
            traces = fetch_tempo_traces()

            incident_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "alert": alert_data,
                "endpoint": endpoint,
                "logs": logs,
                "traces": traces
            }

            ts = int(time.time())
            incident_file = INCIDENTS_DIR / f"incident_{ts}.json"
            incident_file.write_text(json.dumps(incident_record, indent=2))
            LATEST_INCIDENT_FILE.write_text(json.dumps(incident_record, indent=2))

            agent_response = run_agent(alert_data, logs, traces, endpoint)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "ok",
                "incident_saved": str(incident_file),
                "agent_response": agent_response
            }).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        elif self.path == "/latest-response":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            if LATEST_RESPONSE_FILE.exists():
                self.wfile.write(LATEST_RESPONSE_FILE.read_bytes())
            else:
                self.wfile.write(b"No response yet")
        else:
            self.send_response(404)
            self.end_headers()


def run():
    server = http.server.HTTPServer(("0.0.0.0", PORT), AlertHandler)
    logger.info(f"Incident responder listening on port {PORT}...")
    server.serve_forever()


if __name__ == "__main__":
    run()
