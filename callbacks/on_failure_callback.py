import yaml
import os
from airflow.utils.email import send_email
import urllib.parse

ALERT_CONFIG_PATH = os.getenv("ALERT_USERS_LIST_PATH", "/home/gamma/env/alert_users.yaml")


def load_alert_config():
    with open(ALERT_CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def get_recipients(keyword: str) -> list[str]:
    config = load_alert_config()
    matched_groups = set()

    for rule in config.get("rules", []):
        if rule["pattern"] == keyword:
            matched_groups.update(rule["notify"])

    emails = set()
    for group in matched_groups:
        group_data = config["groups"].get(group)
        if group_data:
            emails.update(group_data.get("emails", []))
    return sorted(emails)



def notify_email(context):
    task = context["task_instance"]
    dag_id = task.dag_id
    task_id = task.task_id
    run_id = task.run_id
    execution_date = context.get("execution_date")

    # URL-encode i parametri per sicurezza
    base_url = "http://localhost:8080"
    query = urllib.parse.urlencode({
        "execution_date": execution_date.isoformat(),
        "tab": "logs",
        "dag_run_id": run_id,
        "task_id": task_id
    })
    log_url = f"{base_url}/dags/{dag_id}/grid?{query}"
    
    # Percorso log locale (personalizzabile)
    log_path = f"/home/gamma/airflow/logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt=1.log"
    if not os.path.exists(log_path):
        log_preview = "⚠️ Log file not found."
    else:
        with open(log_path, "r") as f:
            lines = f.readlines()[-30:]  # Ultime 30 righe
            log_preview = "".join(lines)
            log_preview = log_preview.replace("<", "&lt;").replace(">", "&gt;")  # Escaping HTML

    recipients = get_recipients("ALERT_FAIL")
    if not recipients:
        return  # no recipients, skip

    subject = f"[ALERT] Task {task.task_id} in DAG {task.dag_id} has failed"
    html_content = f"""
    <html>
      <body style="font-family:Arial, sans-serif; font-size:14px; color:#333;">
        <h2 style="color:#c0392b;">⚠️ Task Failure Alert</h2>
        <table style="border-collapse:collapse;">
          <tr><td><strong>DAG:</strong></td><td>{dag_id}</td></tr>
          <tr><td><strong>Task:</strong></td><td>{task_id}</td></tr>
          <tr><td><strong>Execution Time:</strong></td><td>{execution_date}</td></tr>
          <tr><td><strong>Log URL:</strong></td><td><a href="{log_url}">{log_url}</a></td></tr>
        </table>
        <h3 style="margin-top:20px;">🔍 Log Preview</h3>
        <pre style="background-color:#f5f5f5; padding:10px; border:1px solid #ccc; max-height:300px; overflow:auto;">
{log_preview}
        </pre>
        <p style="color:#888; font-size:12px;">Full log available at the link above.</p>
      </body>
    </html>
    """

    send_email(to=recipients, subject=subject, html_content=html_content)
