import yaml
from typing import Dict

def load_registry(path: str = 'alert_users.yaml') -> Dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)