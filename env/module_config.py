#!/usr/bin/env python3
"""Parse and validate COSIflow module configuration files.

The shell loader deliberately delegates YAML handling to this module.  The
commands print only validated scalar values so callers never need ``eval``.
"""

from __future__ import annotations

import argparse
import copy
import os
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Any

try:
    import yaml
except ModuleNotFoundError:
    print(
        "module configuration error: PyYAML is required by the module loader",
        file=sys.stderr,
    )
    raise SystemExit(3)


MAX_CONFIG_BYTES = 1024 * 1024
VENV_ROOT = PurePosixPath("/home/gamma/envs")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
PYTHON_VERSION_RE = re.compile(r"^(?:python)?[0-9]+(?:\.[0-9]+){0,2}$")
SAFE_VENV_PATH_RE = re.compile(r"^/[A-Za-z0-9._/-]+$")
INSTALL_MODES = {"container", "environment", "both", "none"}
TOP_LEVEL_KEYS = {"install_mode", "paths", "environments", "default_environment"}
PATH_KEYS = {"dags", "pipeline", "images"}
ENVIRONMENT_KEYS = {
    "requirements",
    "requirements_no_deps",
    "venv_path",
    "enabled",
    "description",
    "python_version",
}


class ConfigError(ValueError):
    """Raised when a module configuration is unsafe or does not match schema."""


class UniqueKeySafeLoader(yaml.SafeLoader):
    """SafeLoader variant that rejects duplicate mapping keys."""


UniqueKeySafeLoader.yaml_implicit_resolvers = copy.deepcopy(
    yaml.SafeLoader.yaml_implicit_resolvers
)
for resolver_key, resolvers in list(UniqueKeySafeLoader.yaml_implicit_resolvers.items()):
    UniqueKeySafeLoader.yaml_implicit_resolvers[resolver_key] = [
        resolver for resolver in resolvers if resolver[0] != "tag:yaml.org,2002:bool"
    ]
UniqueKeySafeLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|false|True|False|TRUE|FALSE)$"),
    list("tTfF"),
)


def _construct_unique_mapping(
    loader: UniqueKeySafeLoader, node: yaml.nodes.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ConfigError("mapping keys must be scalar values") from exc
        if duplicate:
            raise ConfigError(f"duplicate YAML key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping
)


def _fail(message: str) -> ConfigError:
    return ConfigError(message)


def _require_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _fail(f"{field} must be a mapping")
    if not all(isinstance(key, str) for key in value):
        raise _fail(f"{field} keys must be strings")
    return value


def _require_string(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise _fail(f"{field} must be a string")
    if not allow_empty and not value:
        raise _fail(f"{field} must not be empty")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise _fail(f"{field} must be a single-line string without control characters")
    return value


def _validate_identifier(value: str, field: str) -> str:
    if value in {".", ".."} or not IDENTIFIER_RE.fullmatch(value):
        raise _fail(
            f"{field} must match {IDENTIFIER_RE.pattern!r} and must not be '.' or '..'"
        )
    return value


def _validate_module_path(value: Any, field: str) -> str:
    path = _require_string(value, field)
    if "\x00" in path:
        raise _fail(f"{field} contains a NUL byte")
    return path


def _validate_venv_path(value: Any, field: str) -> str:
    path = _require_string(value, field)
    if not SAFE_VENV_PATH_RE.fullmatch(path):
        raise _fail(f"{field} contains unsupported characters")

    normalized = PurePosixPath(os.path.normpath(path))
    if normalized == VENV_ROOT or VENV_ROOT not in normalized.parents:
        raise _fail(f"{field} must be a strict descendant of {VENV_ROOT}")
    return str(normalized)


def load_config(path: str | os.PathLike[str]) -> dict[str, Any]:
    config_path = Path(path)
    try:
        size = config_path.stat().st_size
    except OSError as exc:
        raise _fail(f"cannot read configuration: {exc}") from exc
    if size > MAX_CONFIG_BYTES:
        raise _fail(f"configuration exceeds {MAX_CONFIG_BYTES} bytes")

    try:
        text = config_path.read_text(encoding="utf-8")
        loaded = yaml.load(text, Loader=UniqueKeySafeLoader)
    except (OSError, UnicodeError, yaml.YAMLError, ConfigError) as exc:
        if isinstance(exc, ConfigError):
            raise
        raise _fail(f"invalid YAML: {exc}") from exc

    if loaded is None:
        loaded = {}
    config = _require_mapping(loaded, "configuration")
    unknown = sorted(set(config) - TOP_LEVEL_KEYS)
    if unknown:
        raise _fail(f"unknown top-level key(s): {', '.join(unknown)}")

    mode = config.get("install_mode")
    if mode is not None:
        mode = _require_string(mode, "install_mode")
        if mode not in INSTALL_MODES:
            raise _fail(
                f"install_mode must be one of: {', '.join(sorted(INSTALL_MODES))}"
            )

    paths = _require_mapping(config.get("paths", {}), "paths")
    unknown_paths = sorted(set(paths) - PATH_KEYS)
    if unknown_paths:
        raise _fail(f"unknown paths key(s): {', '.join(unknown_paths)}")
    normalized_paths = {
        key: _validate_module_path(value, f"paths.{key}") for key, value in paths.items()
    }

    environments = _require_mapping(config.get("environments", {}), "environments")
    normalized_environments: dict[str, dict[str, Any]] = {}
    for raw_name, raw_environment in environments.items():
        name = _validate_identifier(raw_name, "environment name")
        environment = _require_mapping(raw_environment, f"environments.{name}")
        unknown_environment_keys = sorted(set(environment) - ENVIRONMENT_KEYS)
        if unknown_environment_keys:
            raise _fail(
                f"unknown key(s) in environments.{name}: "
                + ", ".join(unknown_environment_keys)
            )

        if "requirements" not in environment:
            raise _fail(f"environments.{name}.requirements is required")
        normalized_environment: dict[str, Any] = {
            "requirements": _validate_module_path(
                environment["requirements"], f"environments.{name}.requirements"
            )
        }
        if "requirements_no_deps" in environment:
            normalized_environment["requirements_no_deps"] = _validate_module_path(
                environment["requirements_no_deps"],
                f"environments.{name}.requirements_no_deps",
            )
        if "venv_path" in environment:
            normalized_environment["venv_path"] = _validate_venv_path(
                environment["venv_path"], f"environments.{name}.venv_path"
            )
        else:
            normalized_environment["venv_path"] = str(VENV_ROOT / name)

        enabled = environment.get("enabled", False)
        if not isinstance(enabled, bool):
            raise _fail(f"environments.{name}.enabled must be a boolean")
        normalized_environment["enabled"] = enabled

        if "description" in environment:
            normalized_environment["description"] = _require_string(
                environment["description"], f"environments.{name}.description", allow_empty=True
            )
        if "python_version" in environment:
            version = _require_string(
                environment["python_version"], f"environments.{name}.python_version"
            )
            if not PYTHON_VERSION_RE.fullmatch(version):
                raise _fail(f"environments.{name}.python_version is invalid")
            normalized_environment["python_version"] = version

        normalized_environments[name] = normalized_environment

    environment_targets = {
        name: PurePosixPath(environment["venv_path"])
        for name, environment in normalized_environments.items()
    }
    target_items = list(environment_targets.items())
    for index, (left_name, left_path) in enumerate(target_items):
        for right_name, right_path in target_items[index + 1 :]:
            if left_path == right_path:
                raise _fail(
                    f"environments.{left_name}.venv_path and "
                    f"environments.{right_name}.venv_path must be distinct"
                )
            if left_path in right_path.parents or right_path in left_path.parents:
                raise _fail(
                    f"environments.{left_name}.venv_path and "
                    f"environments.{right_name}.venv_path must not overlap"
                )

    default_environment = config.get("default_environment")
    if default_environment is not None:
        default_environment = _validate_identifier(
            _require_string(default_environment, "default_environment"),
            "default_environment",
        )
        if default_environment not in normalized_environments:
            raise _fail("default_environment must name a configured environment")

    return {
        "install_mode": mode,
        "paths": normalized_paths,
        "environments": normalized_environments,
        "default_environment": default_environment,
    }


def _print_scalar(value: Any) -> None:
    if value is None:
        return
    if isinstance(value, bool):
        print("true" if value else "false")
        return
    if not isinstance(value, str):
        raise _fail("requested value is not scalar")
    print(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("validate")

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument(
        "field", choices=("install_mode", "paths.dags", "paths.pipeline", "paths.images")
    )

    subparsers.add_parser("list-environments")

    env_parser = subparsers.add_parser("get-environment")
    env_parser.add_argument("name")
    env_parser.add_argument("field", choices=tuple(sorted(ENVIRONMENT_KEYS)))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "validate":
            return 0
        if args.command == "get":
            if args.field == "install_mode":
                _print_scalar(config["install_mode"])
            else:
                _print_scalar(config["paths"].get(args.field.split(".", 1)[1]))
            return 0
        if args.command == "list-environments":
            for name in config["environments"]:
                print(name)
            return 0
        if args.command == "get-environment":
            _validate_identifier(args.name, "environment name")
            environment = config["environments"].get(args.name)
            if environment is None:
                raise _fail(f"unknown environment: {args.name}")
            _print_scalar(environment.get(args.field))
            return 0
        raise AssertionError(f"unhandled command: {args.command}")
    except ConfigError as exc:
        print(f"module configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
