# Repetition of the test on Ubuntu 26.04.1 LTS x86_64

This procedure tests both COSIflow deployments (Compose and Kubernetes) end-to-end.

**If you already have a Kubernetes cluster**, you can skip the VM setup entirely:
phases A–C (Compose on a single node) and phases D–E (Kubernetes) each work on any
x86-64 Linux machine with Docker and `kubectl` access to the cluster.
The QEMU VM infrastructure described below exists only to emulate an AMD64 environment
on an Apple Silicon Mac where no real x86-64 hardware is available.

The Ubuntu 24.04 ARM64 VMs from the previous test constitute a separate environment.
On Apple Silicon Macs, AMD64 guests require full emulation.

## Requirements

### Hardware

- Apple Silicon Mac (M1 or later)
- At least 24 GiB of available RAM (19 GiB reserved for the three Kubernetes
  VMs plus QEMU overhead)
- At least 60 GiB of free disk space (four QCOW2 disks: two dynamically
  expanding 110 GiB disks for `cosi-1` and `cosi-4`, two ~20 GiB disks for
  `cosi-2` and `cosi-3`)
- Stable power and network throughout the test (recommended: run
  `caffeinate -i` in a dedicated terminal)

### Software (Mac host)

- macOS with [Homebrew](https://brew.sh) installed
- `qemu` — installed by the guide (`brew install qemu`)
- `ssh`, `shasum`, `curl`, `python3`, `tar` — included in macOS

### Required sources

- This directory (`deployment/`) — contains all support scripts
- The COSIflow repository — this guide lives inside it at `tests/deployment/`
- `infrastructure.json` — used in phase E for
  the Kubernetes deployment; adapt it if the private IP of `cosi-1` differs
  from `192.168.89.11`

### Reference versions

| Component | Version used in this test |
|---|---|
| Ubuntu | 26.04.1 LTS (Resolute) amd64 — `release-20261016` |
| K3s | as specified in `k3s-cluster.sh` |
| Docker | latest stable from the official APT repository |
| QEMU | latest available via Homebrew |

### Expected duration

- Phases A–B (Compose): 30–60 min including image download
- Phase C (GeD DAG): 30–60 min (depends on network speed for DC4 data)
- Phases D–E (Kubernetes): 3–5 hours (TCG software emulation of AMD64 on
  Apple Silicon)

* [QEMU](https://www.qemu.org) is used with Tiny Code Generator (TCG - software emulation engine, it reads the instructions from the virtual operating system and translates them ‘on the fly’ (Just-In-Time) into instructions that your physical computer can understand),
* a NAT network for internet access,
* and a private Ethernet network for the cluster.

The guests can be accessed via SSH on loopback ports 23101–23104.
[`qemu-switch.py`](qemu-switch.py) connects the private NICs (physical or virtual network interface card dedicated exclusively to internal traffic, separate from the public network) to `127.0.0.1:23890` and is launched by `vms.sh` when the first node in the cluster starts up.
Preliminary testing revealed that loopback multicast did not pass packets between guests on this Mac.
The TCP connection keeps traffic confined to the loopback.

**Note (optional):** During the test, leave the Mac powered on and with the lid open.
In a dedicated terminal, run `caffeinate -i` until the test has finished.
The Mac going into sleep mode during staging caused freezes in the VMs and interrupted the Kubernetes test monitor.

## A. Create four new VMs and start the Compose phase

On a Mac, from the `tests/` directory of the repository:

```bash
brew install qemu
GUIDE="$PWD"
export COSIFLOW_RETEST_DIR="$HOME/cosiflow-retest/retest-2026-11-10"
umask 077
mkdir -p "$COSIFLOW_RETEST_DIR"
cd "$COSIFLOW_RETEST_DIR"
base=https://cloud-images.ubuntu.com/releases/resolute/release-20261016
image=ubuntu-26.04-server-cloudimg-amd64.img
curl -fsSL --retry 3 "$base/SHA256SUMS" -o SHA256SUMS
awk -v image="*$image" '$2 == image' SHA256SUMS > image.sha256
test -s image.sha256
curl -fSL --retry 3 "$base/$image" -o "$image.part"
mv "$image.part" "$image"
shasum -a 256 -c image.sha256
vm() { "$GUIDE/deployment/vms.sh" "$@"; }
guest() { ssh -F "$COSIFLOW_RETEST_DIR/ssh_config" "$@"; }
vm prepare
vm start cosi-1 cosi-2 cosi-3
vm wait cosi-1 cosi-2 cosi-3
vm stop cosi-1 cosi-2 cosi-3
vm start cosi-4
vm wait cosi-4
vm status cosi-1 cosi-2 cosi-3 cosi-4
```

[`vms.sh`](vms.sh) is part of the guide.
It creates four independent disks in QCOW2 format and uses the same verified official image as a read-only base.
Download it to a new directory.
To restart existing VMs, use `vm start` without re-downloading or replacing the base disk.
The boot verification requires both `x86_64` and `Ubuntu 26.04.1 LTS`.
The first test revealed a cloud-init race condition because Ubuntu had already enabled the NAT NIC at the time of renaming.
The bootstrap provided here preserves the name assigned by the operating system for that NIC.
Only the cluster’s private NIC is named `cluster0`.
The disks for `cosi-1` and `cosi-4` are 110 GiB dynamically expanding disks, for DC4 data and scientific environments.
The distributed phase uses 19 GiB of RAM for the three VMs.
In the Compose test, only `cosi-4` should be running.

## B. Upload the same candidate build and start Compose

On a Mac, while the A features are still available, create a snapshot of the COSIflow working tree.
The snapshot includes the deployment CLI located in the working tree.
Ignored files, data, local secrets, and the Git directory are excluded.
The `--no-xattrs --no-mac-metadata` flags are required on a Mac.
On the first attempt, the AppleDouble `._*` files were interpreted as Python plugins and SQL migrations, blocking GCN initialization.
Verify that `find ~/cosiflow -type f -name ‘._*’ -print` produces no output on the node.
Save the archive and its SHA-256 so you can reuse the exact same build in the second test.
The FasTP module will be cloned by the CLI to the SHA specified in its `site.yaml`.

```bash
source_dir="$GUIDE/.."
archive="$COSIFLOW_RETEST_DIR/cosiflow-candidate.tar.gz"
git -C "$source_dir" rev-parse HEAD > "$COSIFLOW_RETEST_DIR/cosiflow-base-ref.txt"
git -C "$source_dir" ls-files --cached --others --exclude-standard -z |
  tar -czf "$archive" --no-xattrs --no-mac-metadata -C "$source_dir" --null -T -
shasum -a 256 "$archive" | tee "$COSIFLOW_RETEST_DIR/cosiflow-candidate.sha256"
guest cosi-4 'mkdir -p ~/cosiflow && tar -xzf - -C ~/cosiflow' < "$archive"
guest cosi-4 'bash -s' < "$GUIDE/deployment/install-docker.sh"
guest cosi-4 'docker info >/dev/null && docker compose version'
guest cosi-4 'bash -s' < "$GUIDE/deployment/deploy-compose.sh"
```

[`install-docker.sh`](install-docker.sh) follows the official Docker APT procedure for Ubuntu Resolute.
The second SSH session restores membership in the `docker` group.
[`deploy-compose.sh`](deploy-compose.sh) follows Section 2 of the [COSIflow guide](../../deployment/README.md) with dedicated directories, credentials, and databases.
It stores deployment logs in a private directory.
The test uses LocalExecutor, a single-slot pool, and the GCN client only to initialize the local schema.

## C. Run the two DAGs and switch phases

For Compose, verify on the node that `curl -fsS http://127.0.0.1:18081/health` shows the scheduler and metadatabase as `healthy`.
In Kubernetes, perform the same verification after deployment using the port forwarding described in the COSIflow guide.
The test uses the actual FasTP and downloads the DC4 inputs from the configured scientific source.
The runtime directory must have enough space for the archive and extracted data.

On a Mac, transfer the two verification scripts and run the test inside `cosi-4`:

```bash
tar -czf - --no-xattrs --no-mac-metadata -C "$GUIDE/deployment" science.py airflow-check.py |
  guest cosi-4 'mkdir -p ~/deployment && tar -xzf - -C ~/deployment'
guest cosi-4 'python3 ~/deployment/science.py compose ~/compose-evidence'
```

[`science.py`](science.py) and [`airflow-check.py`](airflow-check.py) verify the executor, the absence of import errors, and the status of `init_pipelines` (GeD, DC4, `tdrss` parameters, 50-second window).
They read the product path from XCom and launch `cosidag_GeD` only on the event initialized with `level=1` and `auto_retrig=false`.
The `TEST SUCCESSFUL ✅` confirmation requires the success of both DAGs, successful GeD finalization, product files, and an entry in the local GCN outbox, with no send attempts.
In Kubernetes, the check also requires that the task pods have completed on the intended compute node.
In case of an error, it retains the execution IDs and logs before any retries.

After the Compose confirmation, save the tests and the newly built images.
The cluster will reuse the same amd64 Airflow base and the same GCN client.
The images contain the build, while data and secrets are in the volumes and
remain on `cosi-4`. Then shut down `cosi-4`:

```bash
guest cosi-4 'tar -czf - -C "$HOME" compose-evidence' > "$COSIFLOW_RETEST_DIR/compose-evidence.tar.gz"
guest cosi-4 'docker save cosiflow-airflow:site cosiflow-gcn-client:prototype' \
  > "$COSIFLOW_RETEST_DIR/compose-base-images.tar"
shasum -a 256 "$COSIFLOW_RETEST_DIR/compose-base-images.tar" \
  > "$COSIFLOW_RETEST_DIR/compose-base-images.sha256"
vm stop cosi-4
vm start cosi-1 cosi-2 cosi-3
vm wait cosi-1 cosi-2 cosi-3
vm status cosi-1 cosi-2 cosi-3 cosi-4
```

## D. Install and test K3s

On a Mac, use `k3s-cluster.sh` for sections 1.2–6 of this guide.
The script checks the private network, installs the version of K3s specified below, and adds two necessary settings for the two QEMU NICs.
`node-ip` assigns 192.168.89.11–13 as the internal node addresses.
`flannel-iface: cluster0` uses the private network for pod traffic.
Tokens are passed between guests via SSH, without writing them to the Mac.
The management client is the `kubectl` included in K3s, run on `cosi-1` with a private copy
of the kubeconfig. `K3S_CONFIG_FILE=/dev/null` applies only to the client and prevents the shared binary from attempting to read the server’s root configuration. [K3s script](k3s-cluster.sh).

The script sets 60-second leases on the three nodes, with renewal within 40 seconds and retries
every 5 seconds for K3s components that have lost leadership during the
recovery of emulated VMs. [Lease configuration](k3s-tcg.yaml).

```bash
"$GUIDE/deployment/k3s-cluster.sh"
guest cosi-1
export KUBECONFIG="$HOME/.kube/config" K3S_CONFIG_FILE=/dev/null
```

In the `cosi-1` terminal, verify the CoreDNS configuration and, if necessary, apply the anti-affinity settings from §6.
Run the DNS and HTTP probes from §7, including the nine paths between nodes, and perform the data collection and cleanup from §10.
Save the logs before performing the cleanup. Sections 6, 7, and 10 marked WORKSTATION are executed in
this terminal in the QEMU path.

## E. Deploy COSIflow on the cluster and repeat the GeD test

Return to the Mac terminal. Reuse the archive from the Compose phase and the
[rehearsal infrastructure manifest](infrastructure.json).
Use new VMs to start with empty databases and NFS directories.
The `cosi-1` node serves NFS and the workers; `cosi-2` hosts the scheduler, web server, and PostgreSQL; `cosi-3` hosts MySQL.
The pool remains at one slot, and the workers’ memory limit is 6 GiB.

```bash
shasum -a 256 -c "$COSIFLOW_RETEST_DIR/cosiflow-candidate.sha256"
guest cosi-1 'mkdir -p ~/cosiflow && tar -xzf - -C ~/cosiflow' < "$archive"
guest cosi-1 'mkdir -m 700 -p ~/.config/cosiflow-kubernetes'
guest cosi-1 'cat > ~/.config/cosiflow-kubernetes/infrastructure.json' \
  < "$GUIDE/deployment/infrastructure.json"
guest cosi-1 'bash -s' < "$GUIDE/deployment/install-docker.sh"
shasum -a 256 -c "$COSIFLOW_RETEST_DIR/compose-base-images.sha256"
guest cosi-1 'docker load' < "$COSIFLOW_RETEST_DIR/compose-base-images.tar"
for node in cosi-2 cosi-3; do
  guest "$node" 'sudo apt-get install -y nfs-common'
done
guest cosi-1 'bash -s -- prepare' < "$GUIDE/deployment/deploy-kubernetes.sh"
for node in cosi-2 cosi-3; do
  guest cosi-1 'cat ~/cosiflow-kubernetes.tar' |
    guest "$node" 'sudo k3s ctr images import -'
done
guest cosi-1 'bash -s -- deploy' < "$GUIDE/deployment/deploy-kubernetes.sh"
```

[`deploy-kubernetes.sh`](deploy-kubernetes.sh) adapts the NFS manifest to the private address of `cosi-1` and to `/srv/cosiflow-shared`, the same directory as seen by the CLI and by the pods via PVC.
It creates the Secret on the cluster and waits for the services.
On the emulated guests, it sets the
PostgreSQL probe timeout to 10 seconds.
The default value of one second caused the endpoint to disappear even while the PostgreSQL process was running.
It builds the amd64 image using the Kubernetes provider on the same Airflow base image already built on `cosi-4`.
Import the image onto all three nodes because the initialization Job can be scheduled on any of them.
Initialize the GCN schema and call the same deployment CLI used in Compose.

The [Kubernetes chapter of the COSIflow guide](../../deployment/README.md#test-3-isolated-kubernetes-installation) describes the prerequisites and retry policies for Jobs.
The script adapts the scheduler parser for single-process emulated guests, allowing 120 seconds for import and 180 seconds for the parsing process.

If the CLI ends with `timed out waiting for the condition`, check the job specified in the message.
On a Mac, replace the Job name with the one shown in the current log.
If it is still active, wait for that same Job before repeating the deployment.
The CLI retains completed Jobs with the same hash and proceeds with the rollout.
If the Job shows as `Failed`, collect the logs and follow the troubleshooting procedure in the Kubernetes chapter.

```bash
install_job=cosiflow-install-a6ab07a2-7c367b20c333
guest cosi-1 bash -s -- "$install_job" <<'BASH'
set -e
export KUBECONFIG="$HOME/.kube/config" K3S_CONFIG_FILE=/dev/null
kubectl -n cosiflow-distributed-test get "job/$1"
kubectl -n cosiflow-distributed-test wait --for=condition=complete "job/$1" --timeout=10800s
BASH
guest cosi-1 'bash -s -- deploy' < "$GUIDE/deployment/deploy-kubernetes.sh"
```

From `cosi-1`, check `/health` using the Service ClusterIP.
Wait until Gunicorn is listening on 8080 and both the scheduler and the metadatabase are `healthy`.
A port-forward opened before Gunicorn may close upon the first connection.
Then launch the two DAGs from the Mac.

```bash
guest cosi-1 'export KUBECONFIG="$HOME/.kube/config" K3S_CONFIG_FILE=/dev/null
  ip=$(kubectl -n cosiflow-distributed-test get svc cosiflow-webserver -o jsonpath="{.spec.clusterIP}")
  curl -fsS --max-time 15 "http://$ip:8080/health"'
tar -czf - --no-xattrs --no-mac-metadata -C "$GUIDE/deployment" science.py airflow-check.py |
  guest cosi-1 'mkdir -p ~/deployment && tar -xzf - -C ~/deployment'
guest cosi-1 'export KUBECONFIG="$HOME/.kube/config" K3S_CONFIG_FILE=/dev/null
  python3 ~/deployment/science.py kubernetes ~/kubernetes-evidence'
guest cosi-1 'tar -czf - -C "$HOME" kubernetes-evidence' > "$COSIFLOW_RETEST_DIR/kubernetes-evidence.tar.gz"
```

If the monitor loses its connection, repeat the `science.py` command with `--resume` and the same evidence directory. 
The script resumes with the IDs that were already recorded.
For Kubernetes, it also repeats any interrupted `kubectl exec` calls up to five times.

If `cosi-1` is recovering after a K3s restart, first verify that another server responds to `/readyz`.
To continue observing the same run from `cosi-1`, you can point a private copy of the kubeconfig to the available server.

```bash
guest cosi-1 'set -e
  umask 077
  export KUBECONFIG="$HOME/.kube/cosiflow-peer.yaml" K3S_CONFIG_FILE=/dev/null
  cp -p "$HOME/.kube/config" "$KUBECONFIG"
  kubectl config set-cluster default --server=https://192.168.89.12:6443
  kubectl --request-timeout=30s get --raw=/readyz
  python3 ~/deployment/science.py kubernetes ~/kubernetes-evidence --resume'
```

If Airflow reports a `postgres` resolution error, check the CoreDNS pods, the PostgreSQL service endpoints, and the resolution from the scheduler pod.
Run the checks on the K3s server that responds to `/readyz`.

```bash
guest cosi-2 'set -e
  sudo k3s kubectl --request-timeout=25s get --raw=/readyz
  sudo k3s kubectl -n kube-system get pods -l k8s-app=kube-dns -o wide
  sudo k3s kubectl -n cosiflow-distributed-test get endpointslice \
    -l kubernetes.io/service-name=postgres
  sudo k3s kubectl -n cosiflow-distributed-test exec deployment/cosiflow-scheduler -- \
    getent hosts postgres'
```

If a guest gets stuck after the host is suspended, save the serial log to `$COSIFLOW_RETEST_DIR/<node>/serial.log` and recover one node at a time from the existing disks.
Wait for the API to respond to `/readyz` and for the node to return to `Ready` before proceeding to the next one.
In the test on November 10–11, `cosi-2` unmounted NFS and shut down K3s during the reboot, then got stuck on `unattended-upgrades.service`.
After confirming on the console that the shutdown had stalled and that K3s was already off, we terminated the node’s QEMU process and restarted the same disk with `vms.sh start cosi-2`.
On the guest, `sudo dpkg --audit` indicated that the kernel package needed to be reconfigured with `sudo dpkg --configure <package>`.

On a cluster started before `k3s-tcg.yaml` was added, install the file in `/etc/rancher/k3s/config.yaml.d/90-retest-tcg.yaml` on each guest.
Restart `k3s` one node at a time and verify `/readyz` and the `Ready` status before moving on to the next one.

If the VM freezes during `stage_all_files`, verify that the container has terminated and check the size of the FITS file. If the extraction is partial and the `.gz` file is available, move the partial FITS file out of `data/raw`. 
On `cosi-1`, use the path to the already downloaded gzip file.

```bash
set -e
gz=/path/to/background.fits.gz
fits=${gz%.gz}
recovery=$(mktemp -d /srv/cosiflow-shared/fits-recovery.XXXXXX)
mv -- "$fits" "$recovery/background-incomplete.fits"
gzip -dc -- "$gz" > "$recovery/background-verified.fits.part"
mv -- "$recovery/background-verified.fits.part" "$fits"
```

`gzip -dc` verifies the CRC and size recorded in the archive.
After the verification, if Airflow shows `failed` for both the run and `stage_all_files`, use the ID saved in `init_pipelines-trigger.json`.
The command requeues the task and the tasks in `upstream_failed` for that run.

```bash
run=manual__retest_kubernetes_init_pipelines_TEST_RUN_ID
guest cosi-1 "export KUBECONFIG=\$HOME/.kube/config K3S_CONFIG_FILE=/dev/null
  kubectl -n cosiflow-distributed-test exec -i deployment/cosiflow-scheduler -- \
    /home/gamma/venv/bin/python - retry-stage init_pipelines $run '{}'" \
  < "$GUIDE/deployment/airflow-check.py"
```

Then resume `science.py` using `--resume`.
The cluster log shows the successful pods and any failed attempts.

When finished, save the status of the VMs, nodes, deployments, pods, and volumes.

```bash
"$GUIDE/deployment/vms.sh" status cosi-1 cosi-2 cosi-3 cosi-4 \
  > "$COSIFLOW_RETEST_DIR/vm-kubernetes-final-state.txt"
guest cosi-2 'set -e
  sudo k3s kubectl --request-timeout=35s get --raw=/readyz
  printf "\n"
  sudo k3s kubectl --request-timeout=35s get nodes -o wide
  sudo k3s kubectl --request-timeout=35s -n cosiflow-distributed-test \
    get deployments,pods,pvc -o wide' > "$COSIFLOW_RETEST_DIR/kubernetes-final.txt"
```

Store the logs for the two modes and the IDs of the four DagRuns separately.
Record the base ref of COSIflow, the hash of the candidate snapshot, the ref of the FasTP module, the VM versions, and the observed deviations.
Use the confirmation `TEST SUCCESSFUL ✅` only after the corresponding phase has succeeded.
