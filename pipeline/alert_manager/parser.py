import re
from typing import List

def parse_errors(log_path: str) -> List[str]:
    error_keywords = ["ERROR", "CRITICAL", "Traceback"]
    matched_lines = []
    with open(log_path, 'r') as f:
        for line in f:
            if any(keyword in line for keyword in error_keywords):
                matched_lines.append(line.strip())
    return matched_lines