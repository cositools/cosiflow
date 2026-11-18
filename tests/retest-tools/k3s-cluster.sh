#!/usr/bin/env bash
set -euo pipefail

: "${COSIFLOW_RETEST_DIR:?Set COSIFLOW_RETEST_DIR to the private experiment directory}"
helpers=$(cd "$(dirname "$0")" && pwd)
ssh_config="$COSIFLOW_RETEST_DIR/ssh_config"
# Wrapper so every SSH command uses the shared ssh_config (key, StrictHostKeyChecking, etc.).
guest() { ssh -F "$ssh_config" "$@"; }

# Pin K3s to a known-good version that includes the TCG memory-limit workaround.
version=v1.37.2+k3s1

# Preflight: verify that all three nodes are healthy and that the private cluster
# network (cluster0, 192.168.89.1x/24) is reachable between every pair of nodes.
# TCG emulation over loopback multicast can silently lose packets — this confirms
# that the qemu-switch.py relay is working before K3s tries to form the cluster.
for n in 1 2 3; do
  guest "cosi-$n" 'hostname; uname -m; nproc; free -h; df -h /; ip -4 -brief address; timedatectl status'
  for peer in 1 2 3; do
    [[ "$n" == "$peer" ]] || guest "cosi-$n" "ping -I cluster0 -c 2 -W 3 192.168.89.1$peer"
  done
  # Download the K3s installer on every node while they can all do it concurrently.
  # The actual install happens later, sequentially, so nodes join at the right time.
  guest "cosi-$n" 'sudo apt-get update && sudo apt-get install -y curl ca-certificates &&
      curl --fail --show-error --location https://get.k3s.io -o /tmp/install-k3s.sh &&
      sudo install -d -m 700 /etc/rancher/k3s'
done

configure() {
  local n=$1
  # Write /etc/rancher/k3s/config.yaml with cluster-role-specific options.
  # cosi-1 bootstraps a new embedded etcd cluster (cluster-init: true).
  # cosi-2 and cosi-3 join as server nodes via the cosi-1 API endpoint.
  {
    if [[ "$n" == 1 ]]; then
      echo 'cluster-init: true'
    else
      echo 'server: https://192.168.89.11:6443'
      # The join token is piped in from cosi-1 immediately before calling configure(),
      # so it is never written to a file on the Mac.
      echo 'token-file: /etc/rancher/k3s/join-token'
    fi
    # bind K3s to the private cluster NIC so control-plane traffic does not go
    # over the shared NAT WAN interface.
    cat <<EOF
node-name: cosi-$n
node-ip: 192.168.89.1$n
flannel-iface: cluster0
write-kubeconfig-mode: "0600"
EOF
  } | guest "cosi-$n" 'sudo install -m 600 /dev/null /etc/rancher/k3s/config.yaml &&
                       sudo tee /etc/rancher/k3s/config.yaml >/dev/null'

  # Apply the TCG-specific drop-in config (resource limits, kubelet flags) from the
  # local k3s-tcg.yaml file.  It is uploaded via stdin to avoid an intermediate file.
  guest "cosi-$n" 'sudo install -d -m 700 /etc/rancher/k3s/config.yaml.d &&
      sudo install -m 600 /dev/null /etc/rancher/k3s/config.yaml.d/90-retest-tcg.yaml &&
      sudo tee /etc/rancher/k3s/config.yaml.d/90-retest-tcg.yaml >/dev/null' \
    < "$helpers/k3s-tcg.yaml"

  # Run the installer, then wait until this node's K3s service is active and the
  # Kubernetes node object is both created and in the Ready condition.
  guest "cosi-$n" "sudo env INSTALL_K3S_VERSION='$version' sh /tmp/install-k3s.sh server &&
      sudo systemctl is-active k3s &&
      sudo k3s kubectl wait --for=create node/cosi-$n --timeout=300s &&
      sudo k3s kubectl wait node/cosi-$n --for=condition=Ready --timeout=300s"
}

# Bootstrap the first node, which creates the cluster and the join token.
configure 1

for n in 2 3; do
  # Stream the join token from cosi-1 directly into cosi-N via SSH pipe.
  # The token is never written to any file on the Mac — it only passes through memory.
  guest cosi-1 'sudo cat /var/lib/rancher/k3s/server/node-token' |
    guest "cosi-$n" 'sudo install -m 600 /dev/null /etc/rancher/k3s/join-token &&
      sudo tee /etc/rancher/k3s/join-token >/dev/null &&
      sudo test -s /etc/rancher/k3s/join-token'
  configure "$n"
done

# Copy the kubeconfig to the ubuntu user's home directory on cosi-1 so that
# deploy-kubernetes.sh can run kubectl from there via SSH.
guest cosi-1 'mkdir -m 700 -p "$HOME/.kube" &&
  sudo install -o "$(id -u)" -g "$(id -g)" -m 600 /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"'

# Final cluster validation from cosi-1:
# - All three nodes must be Ready.
# - The API server readyz endpoint must return ok.
# - CoreDNS is scaled to 2 replicas for resilience (default is 1).
guest cosi-1 'export KUBECONFIG="$HOME/.kube/config" K3S_CONFIG_FILE=/dev/null
set -e
kubectl version
kubectl get nodes -o wide
kubectl wait node --all --for=condition=Ready --timeout=300s
kubectl get pods -n kube-system -o wide
kubectl get storageclass
kubectl get --raw=/readyz
kubectl -n kube-system scale deployment/coredns --replicas=2
kubectl -n kube-system rollout status deployment/coredns --timeout=180s
kubectl -n kube-system get pods -l k8s-app=kube-dns -o wide'
