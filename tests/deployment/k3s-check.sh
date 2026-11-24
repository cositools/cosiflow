#!/usr/bin/env bash
# Run on cosi-1 against the dedicated three-node rehearsal cluster.
set -euo pipefail
umask 077
configure_dns=false
case "${1:-}" in
  '') ;;
  --configure-dns) configure_dns=true ;;
  *) echo 'Usage: bash k3s-check.sh [--configure-dns]' >&2; exit 2 ;;
esac
[[ $# -le 1 ]] || exit 2
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}" K3S_CONFIG_FILE=/dev/null
base="$HOME/k3s-check"
mkdir -p "$base"
evidence=$(mktemp -d "$base/run.XXXXXX")
exec > >(tee "$evidence/check.log") 2>&1
echo "Evidence: $evidence"
ns=cosiflow-check
created=false
k() {
  case "${1:-}" in
    wait|rollout) kubectl --request-timeout=0 "$@" ;; # bounded by --timeout
    *) kubectl --request-timeout=60s "$@" ;;
  esac
}
collect() {
  k version > "$evidence/version.txt" || return
  k get nodes -o wide > "$evidence/nodes.txt" || return
  k get nodes -o json > "$evidence/nodes.json" || return
  k get pods -n kube-system -o wide > "$evidence/system-pods.txt" || return
  k get storageclass > "$evidence/storage.txt" || return
  if "$created"; then
    k -n "$ns" get deployment,service,pods -o wide > "$evidence/workloads.txt" || return
    k -n "$ns" get pods -o json > "$evidence/pods.json" || return
    k -n "$ns" get events --sort-by=.metadata.creationTimestamp > "$evidence/events.txt" || return
  fi
}
finish() {
  status=$?
  trap - EXIT
  if [[ $status -ne 0 ]]; then
    collect || true
    if "$created"; then
      k -n "$ns" describe pods > "$evidence/pod-diagnostics.txt" 2>&1 || true
      for pod in network-check cross-node-cosi-1 cross-node-cosi-2 cross-node-cosi-3; do
        k -n "$ns" logs "$pod" > "$evidence/$pod.failure.log" 2>&1 || true
      done
      echo "Checks failed; namespace $ns retained for inspection."
    fi
    echo FAILED > "$evidence/result.txt"
  fi
  exit "$status"
}
trap finish EXIT

# Section 6: control plane, system services and DNS placement.
collect
k get --raw=/readyz
k wait node/cosi-1 node/cosi-2 node/cosi-3 --for=condition=Ready --timeout=300s
if "$configure_dns"; then
  k -n kube-system scale deployment/coredns --replicas=2
  k -n kube-system patch deployment coredns --type=merge --patch '
{"spec":{"template":{"spec":{"affinity":{"podAntiAffinity":{"preferredDuringSchedulingIgnoredDuringExecution":[{"weight":100,"podAffinityTerm":{"labelSelector":{"matchLabels":{"k8s-app":"kube-dns"}},"topologyKey":"kubernetes.io/hostname"}}]}}}}}}'
fi
k -n kube-system rollout status deployment/coredns --timeout=180s
k -n kube-system get pods -l k8s-app=kube-dns -o wide
dns_nodes=$(k -n kube-system get pods -l k8s-app=kube-dns \
  -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | sed '/^$/d' | wc -l)
[[ $dns_nodes -ge 2 ]] || { echo 'CoreDNS must occupy at least two nodes; use --configure-dns on this dedicated cluster.'; exit 1; }

# Section 7: refuse to reuse or delete an existing namespace.
k create namespace "$ns"
created=true
cat > "$evidence/kubernetes-check.yaml" <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-check
  namespace: cosiflow-check
spec:
  replicas: 3
  selector:
    matchLabels: {app: web-check}
  template:
    metadata:
      labels: {app: web-check}
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchLabels: {app: web-check}
                topologyKey: kubernetes.io/hostname
      containers:
        - name: web
          image: nginx:1.28.0-alpine
          ports: [{containerPort: 80}]
          resources:
            requests: {cpu: 25m, memory: 32Mi}
            limits: {cpu: 250m, memory: 128Mi}
          readinessProbe:
            httpGet: {path: /, port: 80}
            initialDelaySeconds: 2
            periodSeconds: 3
---
apiVersion: v1
kind: Service
metadata:
  name: web-check
  namespace: cosiflow-check
spec:
  selector: {app: web-check}
  ports: [{port: 80, targetPort: 80}]
YAML
k apply -f "$evidence/kubernetes-check.yaml"
k -n "$ns" rollout status deployment/web-check --timeout=300s
k -n "$ns" get pods -o wide
placement=$(k -n "$ns" get pods -l app=web-check \
  -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort)
[[ "$placement" == $'cosi-1\ncosi-2\ncosi-3' ]] || {
  echo 'Web pods must occupy all three nodes to verify all nine paths.'; exit 1;
}
k -n "$ns" run network-check --image=busybox:1.37.0 --restart=Never --command -- sh -c \
  'nslookup web-check.cosiflow-check.svc.cluster.local && wget -T 10 -qO- http://web-check.cosiflow-check.svc.cluster.local'
k -n "$ns" wait pod/network-check --for=jsonpath='{.status.phase}'=Succeeded --timeout=180s
k -n "$ns" logs network-check | tee "$evidence/network-check.log"
targets=$(k -n "$ns" get pods -l app=web-check -o jsonpath='{range .items[*]}{.status.podIP}{" "}{end}')
for node in cosi-1 cosi-2 cosi-3; do
  overrides=$(printf '{"spec":{"nodeSelector":{"kubernetes.io/hostname":"%s"}}}' "$node")
  k -n "$ns" run "cross-node-$node" --image=busybox:1.37.0 --restart=Never \
    --overrides="$overrides" --env="TARGETS=$targets" --command -- sh -c \
    'for target in $TARGETS; do wget -T 10 -qO /dev/null "http://$target" || exit 1; printf "OK %s\n" "$target"; done'
done
for node in cosi-1 cosi-2 cosi-3; do
  k -n "$ns" wait "pod/cross-node-$node" --for=jsonpath='{.status.phase}'=Succeeded --timeout=180s
  k -n "$ns" logs "cross-node-$node" | tee "$evidence/cross-node-$node.log"
  [[ $(grep -c '^OK ' "$evidence/cross-node-$node.log") -eq 3 ]]
done

# Section 10: evidence must be saved successfully before deleting test resources.
collect
k delete namespace "$ns" --wait=true --timeout=180s
created=false
echo PASSED > "$evidence/result.txt"
echo 'K3s checks PASSED: DNS, service HTTP and all nine node-to-pod paths.'
