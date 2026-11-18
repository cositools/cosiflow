"""Strict site.yaml loading, shared by the deployment CLI and its renderers."""
from __future__ import annotations
from copy import deepcopy
import json
from pathlib import Path
import sys
from urllib.parse import urlsplit

import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'modules'))
from cosidag_execution import normalize_profile


class UniqueLoader(yaml.SafeLoader):
    """Reject duplicate keys rather than silently changing a deployment."""


def _mapping(loader, node, deep=False):
    result = {}
    for key, value in node.value:
        name = loader.construct_object(key, deep=deep)
        if not isinstance(name, str) or name in result:
            raise ValueError('YAML mapping keys must be unique strings')
        result[name] = loader.construct_object(value, deep=deep)
    return result


UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def load_site(filename):
    filename = Path(filename).expanduser().resolve()
    site = yaml.load(filename.read_text(), Loader=UniqueLoader)
    schema = json.loads((ROOT / 'deployment/site.schema.json').read_text())
    errors = sorted(Draft202012Validator(schema).iter_errors(site), key=lambda e: str(list(e.path)))
    if errors:
        e = errors[0]
        raise ValueError(f"{'.'.join(map(str, e.path)) or 'site'}: {e.message}")
    site = deepcopy(site)
    mode = site['deployment']['mode']
    execution = site['execution']
    profiles = execution.get('dag_profiles', {})
    for name, profile in profiles.items():
        if name == '*':
            raise ValueError('Use exact DAG IDs, not wildcard execution profiles')
        normalized = normalize_profile(profile)
        if mode == 'compose' and normalized.get('executor') != 'LocalExecutor':
            raise ValueError('Compose supports only LocalExecutor profiles')
    if mode == 'compose' and execution['default_executor'] != 'LocalExecutor':
        raise ValueError('Compose requires default_executor: LocalExecutor')
    if mode == 'compose':
        for key, host, port in [('metadata_database', 'postgres', 5432), ('gcn_database', 'gcn-mysql', 3306)]:
            endpoint = site['services'][key]
            if endpoint['host'] != host or endpoint['port'] != port:
                raise ValueError('Compose uses its bundled postgres:5432 and gcn-mysql:3306 services')
    if mode == 'kubernetes':
        selector = site[mode].get('workers', {}).get('node_selector', {})
        for profile in profiles.values():
            label = profile.get('node_label', 'kubernetes.io/hostname')
            if profile.get('primary_node') and label in selector and selector[label] not in [profile['primary_node'], *profile.get('fallback_nodes', [])]:
                raise ValueError('Worker selector conflicts with a DAG node affinity profile')
        for profile in [site[mode].get('workers', {}), *site[mode].get('components', {}).values()]:
            normalize_profile({'executor': 'KubernetesExecutor', 'resources': profile.get('resources', {})})
    section = site[mode]
    path_keys = ['workspace', 'secrets_env_file'] if mode == 'compose' else ['shared_path']
    for key in path_keys:
        p = Path(section[key]).expanduser()
        section[key] = str((filename.parent / p).resolve() if not p.is_absolute() else p.resolve())
    base = Path(section['workspace'] if mode == 'compose' else section['shared_path'])
    if base == Path('/') or base == ROOT or ROOT in base.parents:
        raise ValueError('Runtime workspace/shared_path must be outside the source checkout')
    names = set()
    for module in site['module-pipelines']:
        if module['name'] in names:
            raise ValueError('Duplicate module-pipelines name')
        names.add(module['name'])
        if (module['repository'].startswith('https://') and urlsplit(module['repository']).username is not None) or urlsplit(module['repository']).password is not None:
            raise ValueError('Repository credentials must not be embedded in site.yaml URLs')
        path = Path(module['config'])
        if path.is_absolute() or '..' in path.parts or '\\' in module['config']:
            raise ValueError('Module config must be a relative path inside its repository')
    return site


def runtime_root(site):
    mode = site['deployment']['mode']
    return Path(site[mode]['workspace' if mode == 'compose' else 'shared_path'])
