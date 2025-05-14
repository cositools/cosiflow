import hashlib

def hash_line(line: str) -> str:
    return hashlib.sha256(line.encode()).hexdigest()
