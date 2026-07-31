from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


class CosiNoticeValidator:
    def __init__(self, schema_root: Path, schema_path: Path):
        self.schema_root = schema_root
        self.schema_path = schema_path
        self._validator = self._build_validator()

    def validate(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        errors = []
        for error in sorted(self._validator.iter_errors(payload), key=str):
            errors.append(
                {
                    "message": error.message,
                    "path": list(error.absolute_path),
                    "schema_path": list(error.absolute_schema_path),
                }
            )
        return errors

    def _build_validator(self) -> Draft202012Validator:
        resources = []
        for path in self.schema_root.rglob("*.schema.json"):
            data = json.loads(path.read_text(encoding="utf-8"))
            schema_id = data.get("$id")
            if schema_id:
                resources.append((schema_id, Resource.from_contents(data)))
        registry = Registry().with_resources(resources)
        root_schema = json.loads(self.schema_path.read_text(encoding="utf-8"))
        return Draft202012Validator(
            root_schema,
            registry=registry,
            format_checker=FormatChecker(),
        )


def enforce_prototype_safety(payload: dict[str, Any], topic: str, allowlist: list[str], require_test_topics: bool) -> list[str]:
    errors: list[str] = []
    if payload.get("mission") != "COSI":
        errors.append("mission must be COSI")
    if payload.get("alert_tense") not in {"test", "injection"}:
        errors.append("alert_tense must be test or injection for this prototype")
    if topic not in allowlist:
        errors.append(f"topic {topic!r} is not in GCN_TOPIC_ALLOWLIST")
    if require_test_topics and "test" not in topic.lower():
        errors.append("topic must contain 'test' while GCN_REQUIRE_TEST_TOPICS=true")
    classification = payload.get("classification")
    if isinstance(classification, dict):
        total = 0.0
        for key, value in classification.items():
            try:
                probability = float(value)
            except (TypeError, ValueError):
                errors.append(f"classification probability for {key!r} is not numeric")
                continue
            if probability < 0 or probability > 1:
                errors.append(f"classification probability for {key!r} is outside [0, 1]")
            total += probability
        if classification and abs(total - 1.0) > 0.05:
            errors.append(f"classification probabilities sum to {total:.4f}, expected approximately 1")
    return errors
