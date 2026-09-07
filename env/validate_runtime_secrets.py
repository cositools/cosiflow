#!/usr/bin/env python3
"""Fail-fast validation for COSIflow runtime credentials.

The validator never prints secret values.  ``--sqlalchemy-dsn`` is intended
only for command substitution inside the Airflow entrypoint.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
from urllib.parse import quote


REQUIRED_SECRETS = (
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW__WEBSERVER__SECRET_KEY",
    "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY",
    "AIRFLOW__CORE__FERNET_KEY",
    "POSTGRES_PASSWORD",
    "GCN_DB_PASSWORD",
)

COMPROMISED_VALUE_HASHES = {
    "850a67bd6adc78c308db0163b838e0acb76425db8ea0d20887a784cf2a7c193c",
    "ff2f12ec5c6a2e9ef6b61c958ed701c327469190a18075fd909ec2a9b42b94f2",
    "3dd7b3e0e1e6931fb0570dd302b3eb6c6b1bf99f91177cb45e5d9b4699ee4d2c",
    "d80bfb8d0b7705018455c1a74bfed2fe294a9be2f1545a6592d4b36490d9c26e",
    "ff5012169e1929326c565e3f5f3f9d31e3aefe401c8b4945a7a72ff0ae9263e9",
}


def validate_environment() -> list[str]:
    errors: list[str] = []
    values: dict[str, str] = {}
    for name in REQUIRED_SECRETS:
        value = os.getenv(name, "")
        values[name] = value
        if not value:
            errors.append(f"{name} is required and must not be empty")
        elif hashlib.sha256(value.encode()).hexdigest() in COMPROMISED_VALUE_HASHES:
            errors.append(f"{name} still uses a revoked legacy value")

    for name in (
        "AIRFLOW_ADMIN_PASSWORD",
        "AIRFLOW__WEBSERVER__SECRET_KEY",
        "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY",
        "POSTGRES_PASSWORD",
        "GCN_DB_PASSWORD",
    ):
        value = values.get(name, "")
        if value and len(value) < 16:
            errors.append(f"{name} must contain at least 16 characters")

    key_names = (
        "AIRFLOW__WEBSERVER__SECRET_KEY",
        "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY",
        "AIRFLOW__CORE__FERNET_KEY",
    )
    present_keys = [values.get(name, "") for name in key_names if values.get(name)]
    if len(present_keys) != len(set(present_keys)):
        errors.append("Airflow web, internal API, and Fernet keys must be distinct")

    fernet_keys = [item.strip() for item in values.get("AIRFLOW__CORE__FERNET_KEY", "").split(",") if item.strip()]
    if fernet_keys:
        try:
            if any(len(base64.urlsafe_b64decode(key.encode("ascii"))) != 32 for key in fernet_keys):
                raise ValueError
        except Exception:
            errors.append("AIRFLOW__CORE__FERNET_KEY contains an invalid Fernet key")

    for name in ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_DB"):
        if not os.getenv(name, ""):
            errors.append(f"{name} is required and must not be empty")
    try:
        port = int(os.getenv("POSTGRES_PORT", ""))
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        errors.append("POSTGRES_PORT must be an integer between 1 and 65535")

    return errors


def sqlalchemy_dsn() -> str:
    return "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        quote(os.environ["POSTGRES_USER"], safe=""),
        quote(os.environ["POSTGRES_PASSWORD"], safe=""),
        os.environ["POSTGRES_HOST"],
        int(os.environ["POSTGRES_PORT"]),
        quote(os.environ["POSTGRES_DB"], safe=""),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlalchemy-dsn", action="store_true")
    args = parser.parse_args()
    errors = validate_environment()
    if errors:
        for error in errors:
            print(f"configuration error: {error}", file=__import__("sys").stderr)
        return 2
    if args.sqlalchemy_dsn:
        print(sqlalchemy_dsn())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
