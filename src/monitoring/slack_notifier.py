import os
import requests


SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def send_alert(title: str, message: str, severity: str = "warning") -> bool:
    """
    Send a Slack alert to the data-oncall channel.

    severity: 'info' | 'warning' | 'critical'
    """
    if not SLACK_WEBHOOK_URL:
        print(f"[SLACK DISABLED] {title}: {message}")
        return False

    color_map = {"info": "#36a64f", "warning": "#e3b341", "critical": "#f85149"}
    color = color_map.get(severity, "#8b949e")

    payload = {
        "attachments": [
            {
                "color": color,
                "title": f"[{severity.upper()}] {title}",
                "text": message,
                "footer": "fintech-pipeline · data-engineering",
            }
        ]
    }

    response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
    return response.status_code == 200


def send_dlq_alert(dlq_count: int, sample_failure_reason: str) -> bool:
    return send_alert(
        title="DLQ Spike Detected",
        message=f"{dlq_count} events routed to DLQ in the last minute.\nSample reason: `{sample_failure_reason}`",
        severity="critical",
    )


def send_pipeline_success(pipeline_name: str, row_count: int, duration_seconds: float) -> bool:
    return send_alert(
        title=f"Pipeline Complete: {pipeline_name}",
        message=f"Processed {row_count:,} events in {duration_seconds:.1f}s.",
        severity="info",
    )
