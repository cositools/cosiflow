"""Deployment contract and orchestration tests. No live Docker/Kubernetes writes."""
from copy import deepcopy
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'deployment'))
from config import load_site
from bundle import compose_bundle, kubernetes_bundle, write_bundle
import yaml
loader = importlib.machinery.SourceFileLoader('cosiflowctl', str(ROOT / 'deployment/cosiflowctl'))
spec = importlib.util.spec_from_loader(loader.name, loader)
cli = importlib.util.module_from_spec(spec)
loader.exec_module(cli)


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.local = load_site(ROOT / 'deployment/examples/site.compose.yaml')
        self.cluster = load_site(ROOT / 'deployment/examples/site.kubernetes.yaml')

    def load_modified(self, site):
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / 'site.yaml'
            f.write_text(yaml.safe_dump(site))
            return load_site(f)

    def test_reject_wrong_executor_for_compose(self):
        for modification in [dict(default_executor='KubernetesExecutor'), dict(dag_profiles={'science': {'executor': 'KubernetesExecutor'}})]:
            s = deepcopy(self.local)
            s['execution'].update(modification)
            with self.subTest(modification=modification), self.assertRaises(ValueError):
                self.load_modified(s)

    def test_reject_unknown_fields_credentials_and_unsafe_module_paths(self):
        variants = []
        for mutation in [lambda s:s.update(password='secret'),
                         lambda s:s['module-pipelines'][0].update(config='../escape.yaml'),
                         lambda s:s['module-pipelines'][0].update(repository='https://user:secret@example.org/repo.git'),
                         lambda s:s['module-pipelines'][0].update(ref='main'),
                         lambda s:s['module-pipelines'].append(deepcopy(s['module-pipelines'][0])),
                         lambda s:s['compose'].update(workspace=str(ROOT / 'data'))]:
            s=deepcopy(self.local);mutation(s);variants.append(s)
        for s in variants:
            with self.subTest(site=s), self.assertRaises(ValueError):self.load_modified(s)

    def test_worker_selector_must_intersect_dag_affinity(self):
        s=deepcopy(self.cluster)
        s['kubernetes']['workers']['node_selector']={'kubernetes.io/hostname':'other-node'}
        with self.assertRaisesRegex(ValueError,'conflicts'):self.load_modified(s)

    def test_duplicate_yaml_keys_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            f=Path(tmp)/'site.yaml';f.write_text('config_version: 1\nconfig_version: 2\n')
            with self.assertRaises(ValueError):load_site(f)

    def test_two_generic_module_install_jobs_are_pinned_and_ordered(self):
        s=deepcopy(self.cluster)
        s['module-pipelines'].append(dict(name='another-pipeline',repository='https://example.org/other.git',ref='a'*40,config='env/other.config.yaml'))
        bundle=kubernetes_bundle(s)
        jobs=bundle['module-install.json']['items'];self.assertEqual(len(jobs),2)
        self.assertNotEqual(jobs[0]['metadata']['name'],jobs[1]['metadata']['name'])
        for m,job in zip(s['module-pipelines'],jobs):
            c=job['spec']['template']['spec']['containers'][0]
            self.assertEqual(c['args'],[m['name'],'install','-c',f'/home/gamma/airflow/modules_pool/{m["name"]}/{m["config"]}'])
            self.assertEqual(job['spec']['template']['metadata']['annotations']['cosiflow.io/module-ref'],m['ref'])
            mounts={v['subPath']:v.get('readOnly',False) for v in c['volumeMounts'] if 'subPath' in v}
            self.assertTrue(mounts['modules-pool']);self.assertFalse(mounts['envs'])

    def test_default_executor_worker_memory_and_component_placement(self):
        b=kubernetes_bundle(self.cluster)
        runtime=next(x for x in b['bootstrap.json']['items'] if x['metadata']['name']=='cosiflow-runtime')
        self.assertEqual(runtime['data']['AIRFLOW__CORE__EXECUTOR'],'KubernetesExecutor,LocalExecutor')
        self.assertEqual(runtime['data']['AIRFLOW__KUBERNETES_EXECUTOR__DELETE_WORKER_PODS'],'False')
        files=next(x for x in b['bootstrap.json']['items'] if x['metadata']['name']=='cosiflow-files')
        pod=json.loads(files['data']['pod_template.yaml'])
        self.assertEqual(pod['spec']['nodeSelector'],{'kubernetes.io/hostname':'compute-a'})
        self.assertEqual(pod['spec']['containers'][0]['resources']['limits']['memory'],'6Gi')
        self.assertEqual(b['runtime.json']['items'][0]['spec']['template']['spec']['nodeSelector'],{'kubernetes.io/hostname':'services-a'})

    def test_job_names_change_with_install_ref_and_pool_but_are_stable(self):
        first=kubernetes_bundle(self.cluster)
        self.assertEqual(first,kubernetes_bundle(self.cluster))
        s=deepcopy(self.cluster);s['execution']['default_pool_slots']=2
        self.assertNotEqual(first['init.json']['items'][0]['metadata']['name'],kubernetes_bundle(s)['init.json']['items'][0]['metadata']['name'])
        s=deepcopy(self.cluster);s['module-pipelines'][0]['ref']='a'*40
        self.assertNotEqual(first['module-install.json'],kubernetes_bundle(s)['module-install.json'])

    def test_changed_database_or_profiles_rebootstrap_and_roll_runtime(self):
        before=kubernetes_bundle(self.cluster)
        for change in ['database','profile']:
            s=deepcopy(self.cluster)
            if change=='database':s['services']['metadata_database']['database']='another_metadata_database'
            else:s['execution']['dag_profiles']['cosidag_GeD']['primary_node']='compute-b'
            after=kubernetes_bundle(s)
            self.assertNotEqual(before['init.json']['items'][0]['metadata']['name'],after['init.json']['items'][0]['metadata']['name'])
            self.assertNotEqual(before['runtime.json']['items'][0]['spec']['template']['metadata'],after['runtime.json']['items'][0]['spec']['template']['metadata'])

    def test_compose_has_isolated_paths_no_fixed_container_names_no_kafka_worker(self):
        bundle=compose_bundle(self.local);services=bundle['compose.json']['services']
        for s in services.values():self.assertNotIn('container_name',s)
        for name in ['airflow-init','airflow-scheduler','airflow-webserver']:
            s=services[name];self.assertEqual(s['environment']['AIRFLOW__CORE__EXECUTOR'],'LocalExecutor')
            self.assertFalse(any('../../fast-transient' in v for v in s['volumes']))
            self.assertTrue(any(self.local['compose']['workspace']+'/modules-pool:' in v for v in s['volumes']))
        self.assertEqual(services['gcn-client']['environment']['GCN_CONSUMER_ENABLED'],'false')
        self.assertEqual(services['gcn-client']['profiles'],['gcn-schema-only'])
        self.assertNotIn('GCN_CLIENT_SECRET',services['gcn-client']['environment'])
        self.assertIn('127.0.0.1:18081:8080',services['airflow-webserver']['ports'])

    def test_compose_source_and_state_do_not_alias_two_sites(self):
        a=compose_bundle(self.local)
        s=deepcopy(self.local);s['compose']['workspace']='/tmp/another-site'
        b=compose_bundle(s)
        self.assertNotEqual(a['compose.json']['services']['postgres']['volumes'],b['compose.json']['services']['postgres']['volumes'])

    def test_render_does_not_mutate_site_or_access_external_tools(self):
        for site in [self.local,self.cluster]:
            original=deepcopy(site)
            with tempfile.TemporaryDirectory() as tmp, patch('subprocess.run',side_effect=AssertionError('external action')):
                out=Path(tmp)/'bundle';write_bundle(site,out)
                self.assertEqual(site,original)
                with self.assertRaises(FileExistsError):write_bundle(site,out)
                self.assertFalse(any(x.get('kind')=='Secret' for f in out.glob('*.json') for x in json.loads(f.read_text()).get('items',[])))

    def test_completed_jobs_are_kept_without_apply_or_delete(self):
        with tempfile.TemporaryDirectory() as tmp:
            f=Path(tmp)/'init.json';f.write_text(json.dumps(kubernetes_bundle(self.cluster)['init.json']))
            with patch.object(cli,'completed_job',return_value=True),patch.object(cli,'run') as r:
                cli.apply_jobs(self.cluster,f);r.assert_not_called()

    def test_failed_jobs_require_deliberate_operator_retry(self):
        response=subprocess.CompletedProcess([],0,stdout=json.dumps({'status':{'failed':1}}))
        with patch.object(cli,'run',return_value=response),self.assertRaisesRegex(ValueError,'previously failed'):
            cli.completed_job(self.cluster,'job-test')

    def test_stop_and_deploy_refuse_active_workers(self):
        response=subprocess.CompletedProcess([],0,stdout=json.dumps({'items':[{'status':{'phase':'Running'}}]}))
        with patch.object(cli,'run',return_value=response),self.assertRaisesRegex(ValueError,'active'):
            cli.active_guard(self.cluster,Path('/tmp/bundle'))

    def test_existing_modified_or_wrong_ref_sources_are_never_reset(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=deepcopy(self.local);s['compose']['workspace']=tmp
            dest=Path(tmp)/'modules-pool'/s['module-pipelines'][0]['name'];(dest/'.git').mkdir(parents=True)
            with patch.object(cli,'run',return_value=subprocess.CompletedProcess([],0,stdout='wrong-ref')) as run:
                with self.assertRaisesRegex(ValueError,'never reset'):cli.prepare(s)
                commands=[call.args[0] for call in run.call_args_list]
                self.assertFalse(any('checkout' in c or 'reset' in c or 'clone' in c for c in commands))

    def test_prepare_clones_into_new_runtime_and_reuses_clean_pinned_checkout(self):
        with tempfile.TemporaryDirectory() as tmp:
            remote=Path(tmp)/'remote';remote.mkdir()
            subprocess.run(['git','init','-q',str(remote)],check=True)
            (remote/'env').mkdir();(remote/'env/module.yaml').write_text('install_mode: none\n')
            subprocess.run(['git','-C',str(remote),'add','.'],check=True)
            subprocess.run(['git','-C',str(remote),'-c','user.name=Test','-c','user.email=test@example.invalid','commit','-qm','fixture'],check=True)
            sha=subprocess.check_output(['git','-C',str(remote),'rev-parse','HEAD'],text=True).strip()
            site=deepcopy(self.local);site['compose']['workspace']=str(Path(tmp)/'runtime')
            # A local fixture substitutes for network Git only inside this unit test.
            site['module-pipelines']=[dict(name='fixture',repository=str(remote),ref=sha,config='env/module.yaml')]
            cli.prepare(site);cli.prepare(site)
            clone=Path(site['compose']['workspace'])/'modules-pool/fixture'
            self.assertEqual(subprocess.check_output(['git','-C',str(clone),'rev-parse','HEAD'],text=True).strip(),sha)
            self.assertFalse((Path(site['compose']['workspace'])/'envs/cosipy').exists())

    def test_install_is_explicit_and_compose_plan_contains_every_deployment_step(self):
        out=Path('/tmp/bundle')
        normal=cli.compose_commands(self.local,out,False)
        first=cli.compose_commands(self.local,out,True)
        self.assertFalse(any('/home/gamma/hot_load_module.sh' in c for c in normal))
        self.assertEqual(len(first),len(normal)+1)
        self.assertEqual(first[0][-4:],['--wait','postgres','gcn-mysql','mailhog'])
        self.assertIn('init-db',first[2]);self.assertIn('/home/gamma/hot_load_module.sh',first[3])
        self.assertEqual(first[-1][-2:],['airflow-scheduler','airflow-webserver'])

    def test_kubernetes_deploy_waits_bootstrap_and_installers_before_runtime(self):
        events=[]
        def fake_run(command,**kwargs):
            events.append(('command',command))
            return subprocess.CompletedProcess(command,0,stdout='')
        with patch.object(cli,'verify_sources'),patch.object(cli,'active_guard'),patch.object(cli,'cluster_prerequisites'),patch.object(cli,'run',side_effect=fake_run),patch.object(cli,'apply_jobs',side_effect=lambda s,f:events.append(('jobs',f.name))):
            cli.deploy(self.cluster,Path('/tmp/bundle'),True)
        self.assertEqual(events[0][1][-1],'/tmp/bundle/bootstrap.json')
        self.assertEqual(events[1],('jobs','init.json'));self.assertEqual(events[2],('jobs','module-install.json'))
        self.assertEqual(events[3][1][-1],'/tmp/bundle/runtime.json')

    def test_context_is_explicit_for_every_kubectl_command(self):
        self.assertEqual(cli.kubectl(self.cluster)[:3],['kubectl','--context','CHANGE-ME-SITE-CONTEXT'])


if __name__=='__main__':unittest.main()
