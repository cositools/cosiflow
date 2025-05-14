import sys
import os
sys.path.append('/shared_dir/pipeline')
from .parser import parse_errors
from .notifier import send_notification
from .registry import load_registry
from .utils import hash_line

def run_monitor():
    log_path = os.environ.get('ALERT_LOG_PATH', '/home/gamma/workspace/log/data_pipeline.log')
    alert_users_path = os.environ.get('ALERT_USERS_LIST_PATH', '/home/gamma/env/alert_users.yaml')
    seen_hashes = set()  # In futuro si può salvare su file o Redis
    errors = parse_errors(log_path)
    registry = load_registry(alert_users_path)

    for line in errors:
        h = hash_line(line)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)

        for rule in registry.get("rules", []):
            if rule["pattern"] in line:
                for group in rule["notify"]:
                    recipients = registry["groups"].get(group, {}).get("emails", [])
                    if recipients:
                        send_notification(line, recipients)