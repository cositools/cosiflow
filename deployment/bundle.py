"""Offline generation using COSIflow's existing Compose and Kubernetes recipes."""
from __future__ import annotations
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path

import yaml
from config import ROOT, runtime_root

spec = importlib.util.spec_from_file_location('cosiflow_cluster_renderer', ROOT / 'deployment/kubernetes/render.py')
renderer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(renderer)


def json_list(items):
    return {'apiVersion': 'v1', 'kind': 'List', 'items': items}


def cluster_site(site):
    k = site['kubernetes']
    services = site['services']
    result = dict(namespace=k['namespace'], pvc=k['shared_pvc'], secret=k['runtime_secret'],
                  image=site['application']['image'], uid=site['runtime']['uid'], gid=site['runtime']['gid'],
                  public_base_url=site['application']['public_url'],
                  smtp_host=services['smtp']['host'], smtp_port=str(services['smtp']['port']),
                  task_profiles=site['execution'].get('dag_profiles', {}))
    for section, prefix in [('metadata_database', 'postgres'), ('gcn_database', 'gcn_db')]:
        for key, target in [('host', 'host'), ('port', 'port'), ('user', 'user'), ('database', 'db' if prefix == 'postgres' else 'name')]:
            if prefix == 'gcn_db' and key == 'port':
                continue  # Legacy renderer fixes this port; overridden below.
            result[f'{prefix}_{target}'] = str(services[section][key])
    for name in ['image_pull_policy', 'image_pull_secrets']:
        if name in k:
            result[name] = k[name]
    for name in ['scheduler', 'webserver']:
        result[name + '_resources'] = k.get('components', {}).get(name, {}).get('resources', {})
    result['worker_resources'] = k.get('workers', {}).get('resources', {})
    return result


def _job_name(job):
    digest = hashlib.sha256(json.dumps(job['spec'], sort_keys=True).encode()).hexdigest()[:12]
    job['metadata']['name'] += '-' + digest
    return job


def kubernetes_bundle(site):
    legacy = renderer.render(cluster_site(site))
    k = site['kubernetes']
    config = next(o for o in legacy['bootstrap.json'] if o['metadata']['name'] == 'cosiflow-runtime')
    default = site['execution']['default_executor']
    config['data']['AIRFLOW__CORE__EXECUTOR'] = ','.join([default, 'LocalExecutor' if default == 'KubernetesExecutor' else 'KubernetesExecutor'])
    config['data']['AIRFLOW__KUBERNETES_EXECUTOR__DELETE_WORKER_PODS'] = str(not k.get('workers', {}).get('retain_completed_pods', False))
    config['data']['AIRFLOW__WEBSERVER__WORKERS'] = '2'
    config['data']['GCN_DB_PORT'] = str(site['services']['gcn_database']['port'])
    for key in ['starttls', 'ssl']:
        config['data']['AIRFLOW__SMTP__SMTP_' + key.upper()] = str(site['services']['smtp'].get(key, False))
    files = next(o for o in legacy['bootstrap.json'] if o['metadata']['name'] == 'cosiflow-files')
    template = json.loads(files['data']['pod_template.yaml'])
    template['spec']['nodeSelector'] = k.get('workers', {}).get('node_selector', {})
    files['data']['pod_template.yaml'] = json.dumps(template)
    checksum = hashlib.sha256(json.dumps([config['data'], files['data']], sort_keys=True).encode()).hexdigest()
    for deployment in legacy['runtime.json']:
        if deployment['kind'] == 'Deployment':
            deployment['spec']['template']['metadata'].setdefault('annotations', {})['cosiflow.io/config-checksum'] = checksum
            mode = deployment['metadata']['name'].removeprefix('cosiflow-')
            deployment['spec']['template']['spec']['nodeSelector'] = k.get('components', {}).get(mode, {}).get('node_selector', {})
    init = legacy['init.json'][0]
    init['spec']['template']['metadata'] = {'annotations': {'cosiflow.io/config-checksum': checksum}}
    container = init['spec']['template']['spec']['containers'][0]
    pool = site['execution']['default_pool_slots']
    container['command'] = ['bash', '-ec']
    container['args'] = ['bash /home/gamma/entrypoint-airflow.sh init; '
        'export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(python /home/gamma/validate_runtime_secrets.py --scope init --sqlalchemy-dsn)"; '
        f'airflow pools set default_pool {pool} "Configured deployment pool"']
    _job_name(init)
    jobs = []
    installer_template = legacy['module-install.json'][0]
    for module in site['module-pipelines']:
        job = deepcopy(installer_template)
        job['metadata']['name'] = 'cosiflow-install-' + hashlib.sha256(module['name'].encode()).hexdigest()[:8]
        job['spec']['template']['metadata'] = {'annotations': {'cosiflow.io/module-ref': module['ref'], 'cosiflow.io/config-checksum': checksum}}
        c = job['spec']['template']['spec']['containers'][0]
        c['args'] = [module['name'], 'install', '-c', '/home/gamma/airflow/modules_pool/' + module['name'] + '/' + module['config']]
        job['spec']['template']['spec']['nodeSelector'] = k.get('components', {}).get('scheduler', {}).get('node_selector', {})
        jobs.append(_job_name(job))
    return {'bootstrap.json': json_list(legacy['bootstrap.json']), 'init.json': json_list([init]),
            'module-install.json': json_list(jobs), 'runtime.json': json_list(legacy['runtime.json'])}


def compose_bundle(site):
    root = runtime_root(site)
    document = yaml.safe_load((ROOT / 'env/docker-compose.yaml').read_text())
    document = {k: v for k, v in document.items() if not k.startswith('x-')}
    for name, service in document['services'].items():
        service.pop('container_name', None)
        if 'build' in service:
            service['build']['context'] = str(ROOT / ('env' if name.startswith('airflow-') else '.'))
            if name.startswith('airflow-'):
                service['build']['args'].update(UID=str(site['runtime']['uid']), GID=str(site['runtime']['gid']))
        if name.startswith('airflow-'):
            service['image'] = site['application']['image']
            volumes = []
            for sub, target in renderer.MOUNTS.items():
                # Explicit installer runs in a one-shot scheduler container;
                # the loader writes only envs/dags/pipeline during installation.
                volumes.append(f'{root / sub}:{target}' + (':ro' if sub == 'modules-pool' else ''))
            for sub in ['plugins', 'modules', 'callbacks']:
                volumes.append(f'{ROOT / sub}:/home/gamma/airflow/{sub}:ro')
            volumes += [f'{ROOT / "env/airflow.cfg"}:/home/gamma/airflow/airflow.cfg:ro',
                        f'{ROOT / "env/hot_load_module.sh"}:/home/gamma/hot_load_module.sh:ro',
                        f'{root / "task-execution.json"}:/opt/cosiflow/task-execution.json:ro']
            service['volumes'] = volumes
            env = service['environment']
            env.update(AIRFLOW__CORE__EXECUTOR='LocalExecutor', COSIFLOW_TASK_EXECUTION_FILE='/opt/cosiflow/task-execution.json')
            env.update(POSTGRES_HOST='postgres', POSTGRES_PORT='5432',
                       POSTGRES_USER=site['services']['metadata_database']['user'], POSTGRES_DB=site['services']['metadata_database']['database'])
            if name != 'airflow-init':
                env.update(GCN_DB_HOST='gcn-mysql', GCN_DB_PORT='3306',
                    GCN_DB_USER=site['services']['gcn_database']['user'], GCN_DB_NAME=site['services']['gcn_database']['database'],
                    AIRFLOW_PUBLIC_BASE_URL=site['application']['public_url'], AIRFLOW__WEBSERVER__WORKERS='2',
                    AIRFLOW__SMTP__SMTP_HOST=site['services']['smtp']['host'], AIRFLOW__SMTP__SMTP_PORT=str(site['services']['smtp']['port']),
                    GCN_PRODUCER_ENABLED='false', GCN_DRY_RUN='true')
            if name != 'airflow-init':
                for key in ['starttls', 'ssl']:
                    env['AIRFLOW__SMTP__SMTP_' + key.upper()] = str(site['services']['smtp'].get(key, False))
            if name == 'airflow-scheduler':
                service['healthcheck']['test'] = ['CMD-SHELL', 'export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(python /home/gamma/validate_runtime_secrets.py --scope runtime --sqlalchemy-dsn)"; airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
        elif name in ['postgres', 'gcn-mysql']:
            target = '/var/lib/postgresql/data' if name == 'postgres' else '/var/lib/mysql'
            service['volumes'] = [f'{root / (name + "-data")}:{target}']
    services = document['services']
    services['postgres']['environment'].update(POSTGRES_USER=site['services']['metadata_database']['user'], POSTGRES_DB=site['services']['metadata_database']['database'])
    services['gcn-mysql']['environment'].update(MYSQL_USER=site['services']['gcn_database']['user'], MYSQL_DATABASE=site['services']['gcn_database']['database'])
    services['gcn-client']['environment'] = dict(GCN_DB_HOST='gcn-mysql', GCN_DB_PORT='3306', GCN_DB_USER=site['services']['gcn_database']['user'], GCN_DB_NAME=site['services']['gcn_database']['database'], GCN_DB_PASSWORD='${GCN_DB_PASSWORD:?GCN_DB_PASSWORD is required}', GCN_CONSUMER_ENABLED='false', GCN_PRODUCER_ENABLED='false', GCN_DRY_RUN='true')
    c = site['compose']
    services['airflow-webserver']['ports'] = [f'127.0.0.1:{c.get("web_port", 8080)}:8080']
    services['mailhog']['ports'] = [f'127.0.0.1:{c.get("smtp_port", 1025)}:1025', f'127.0.0.1:{c.get("mail_web_port", 8025)}:8025']
    services['gcn-client']['profiles'] = ['gcn-schema-only']
    return {'compose.json': document, 'task-execution.json': site['execution'].get('dag_profiles', {})}


def write_bundle(site, output):
    output = Path(output)
    artifacts = compose_bundle(site) if site['deployment']['mode'] == 'compose' else kubernetes_bundle(site)
    output.mkdir(parents=True, exist_ok=False)
    for name, document in artifacts.items():
        (output / name).write_text(json.dumps(document, indent=2) + '\n')
    # No site secrets are loaded or serialized into the bundle.
    (output / 'site.resolved.json').write_text(json.dumps(site, indent=2) + '\n')
    return output
