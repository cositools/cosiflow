#!/usr/bin/env python3
"""Create or rotate the ignored local .env without disclosing secret values."""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import secrets
from pathlib import Path


GENERATORS = {
    "AIRFLOW_ADMIN_PASSWORD": lambda: secrets.token_urlsafe(32),
    "POSTGRES_PASSWORD": lambda: secrets.token_urlsafe(32),
    "GCN_DB_PASSWORD": lambda: secrets.token_urlsafe(32),
    "GCN_MYSQL_ROOT_PASSWORD": lambda: secrets.token_urlsafe(32),
    "AIRFLOW__WEBSERVER__SECRET_KEY": lambda: secrets.token_urlsafe(48),
    "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY": lambda: secrets.token_urlsafe(48),
    "AIRFLOW__CORE__FERNET_KEY": lambda: base64.urlsafe_b64encode(os.urandom(32)).decode("ascii"),
}
REVOKED_HASHES = {
    "850a67bd6adc78c308db0163b838e0acb76425db8ea0d20887a784cf2a7c193c",
    "ff2f12ec5c6a2e9ef6b61c958ed701c327469190a18075fd909ec2a9b42b94f2",
    "3dd7b3e0e1e6931fb0570dd302b3eb6c6b1bf99f91177cb45e5d9b4699ee4d2c",
    "d80bfb8d0b7705018455c1a74bfed2fe294a9be2f1545a6592d4b36490d9c26e",
    "ff5012169e1929326c565e3f5f3f9d31e3aefe401c8b4945a7a72ff0ae9263e9",
}


def load_env(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    values: dict[str, str] = {}
    for line in lines:
        if line and not line.lstrip().startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            values[name] = value
    return lines, values


def update_lines(lines: list[str], replacements: dict[str, str]) -> list[str]:
    result: list[str] = []
    remaining = dict(replacements)
    for line in lines:
        name = line.split("=", 1)[0] if "=" in line else ""
        if name in remaining:
            result.append(f"{name}={remaining.pop(name)}")
        else:
            result.append(line)
    if result and result[-1] != "":
        result.append("")
    result.extend(f"{name}={value}" for name, value in remaining.items())
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--rotate", action="store_true", help="replace every generated local secret")
    args = parser.parse_args()
    path = Path(args.env_file)
    lines, values = load_env(path)
    replacements = {}
    for name, generator in GENERATORS.items():
        current = values.get(name, "")
        revoked = hashlib.sha256(current.encode()).hexdigest() in REVOKED_HASHES
        if args.rotate or not current or revoked:
            replacements[name] = generator()
    path.write_text("\n".join(update_lines(lines, replacements)) + "\n", encoding="utf-8")
    path.chmod(0o600)
    if replacements:
        print("Generated: " + ", ".join(sorted(replacements)))
    else:
        print("All generated secrets were already present and non-legacy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
