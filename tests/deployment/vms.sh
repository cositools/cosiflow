#!/usr/bin/env bash
set -euo pipefail

# COSIFLOW_RETEST_DIR must be set by the caller (the README's shell session).
# All VM disks, cloud-init seeds, SSH config, and logs land in that directory.
: "${COSIFLOW_RETEST_DIR:?Set COSIFLOW_RETEST_DIR to the private experiment directory}"
helpers=$(cd "$(dirname "$0")" && pwd)
cd "$COSIFLOW_RETEST_DIR"
dir=$PWD
image=ubuntu-26.04-server-cloudimg-amd64.img
qemu=${QEMU_BIN:-/opt/homebrew/bin}
# Private network prefix shared by all cluster nodes (192.168.89.1x).
lan=192.168.89

prepare() {
  # Verify the base image against its SHA-256 before creating any disk.
  shasum -a 256 -c image.sha256
  # Generate a dedicated SSH key pair for passwordless access to all guests.
  if [[ ! -e vm-key ]]; then
    ssh-keygen -q -t ed25519 -N '' -C cosiflow-retest -f vm-key
  fi
  # Write a single ssh_config that covers all four guests on loopback ports 23101-23104.
  umask 077
  {
    for n in 1 2 3 4; do
      cat <<EOF
Host cosi-$n
    HostName 127.0.0.1
    Port 2310$n
    User ubuntu
    IdentityFile "$dir/vm-key"
    IdentitiesOnly yes
    UserKnownHostsFile "$dir/known_hosts"
    StrictHostKeyChecking accept-new
    BatchMode yes
    ConnectTimeout 5
EOF
    done
  } > ssh_config

  for n in 1 2 3 4; do
    local guest=cosi-$n disk=60
    # cosi-1 and cosi-4 get 110 GiB each: cosi-1 serves NFS, cosi-4 runs Compose.
    case "$n" in 1|4) disk=110 ;; esac
    mkdir -p "$guest/seed"
    # Create a copy-on-write QCOW2 disk that shares the verified base image read-only.
    if [[ ! -e "$guest/root.qcow2" ]]; then
      "$qemu/qemu-img" create -f qcow2 -F qcow2 -b "$dir/$image" "$guest/root.qcow2" "${disk}G"
    fi
    if [[ -e "$guest/seed.iso" ]]; then continue; fi
    # cloud-init meta-data: instance identity and hostname.
    cat > "$guest/seed/meta-data" <<EOF
instance-id: cosiflow-retest-$guest
local-hostname: $guest
EOF
    # cloud-init user-data: inject the SSH public key; disable password auth.
    cat > "$guest/seed/user-data" <<EOF
#cloud-config
users:
  - default
ssh_authorized_keys:
  - $(cat vm-key.pub)
ssh_pwauth: false
EOF
    # cloud-init network-config: match NICs by MAC address so renaming survives cloud-init.
    # The WAN NIC gets DHCP for internet access; the cluster NIC is static (cosi-4 has no cluster NIC).
    cat > "$guest/seed/network-config" <<EOF
version: 2
ethernets:
  wan:
    match:
      macaddress: "52:54:00:89:00:0$n"
    dhcp4: true
    dhcp6: false
EOF
    if [[ "$n" != 4 ]]; then
      cat >> "$guest/seed/network-config" <<EOF
  cluster:
    match:
      macaddress: "52:54:00:89:01:0$n"
    set-name: cluster0
    addresses: [$lan.1$n/24]
    dhcp4: false
    dhcp6: false
    link-local: []
EOF
    fi
    # Pack the cloud-init seed files into an ISO image that QEMU mounts as a CD-ROM.
    hdiutil makehybrid -quiet -iso -joliet -default-volume-name cidata \
      -o "$guest/seed.iso" "$guest/seed"
  done
}

# Returns true if the guest's QEMU process is running and owns its disk file.
running() {
  [[ -s "$1/qemu.pid" ]] && ps -p "$(cat "$1/qemu.pid")" -o command= |
    grep -Fq -- "$dir/$1/root.qcow2"
}

# Start the loopback TCP switch that connects the private NICs of cluster nodes.
# qemu-switch.py acts as an Ethernet hub over a single TCP listener on 127.0.0.1:23890.
start_switch() {
  if [[ -s switch.pid ]] && ps -p "$(cat switch.pid)" -o command= |
      grep -Fq -- "$helpers/qemu-switch.py"; then
    return
  fi
  nohup python3 "$helpers/qemu-switch.py" > "$dir/switch.log" 2>&1 </dev/null &
  echo "$!" > switch.pid
  # Wait up to 20 seconds for the switch to report it is listening.
  for ((attempt=1; attempt<=20; attempt++)); do
    if grep -q '^Listening on 127.0.0.1:23890$' "$dir/switch.log"; then return; fi
    sleep 1
  done
  cat "$dir/switch.log" >&2
  return 1
}

start() {
  if running "$guest"; then return; fi
  # Assign RAM and CPUs by role: cosi-1 (NFS+workers) and cosi-4 (Compose) get more resources.
  local memory=4096 cpus=2
  case "$n" in 1|4) memory=10240; cpus=4 ;; 2) memory=5120; cpus=3 ;; esac
  # WAN NIC: NAT with host-forwarding of SSH port 22 to 2310N on loopback.
  local network=(
    -netdev "user,id=wan,net=10.0.$n.0/24,hostfwd=tcp:127.0.0.1:2310$n-:22"
    -device "virtio-net-pci,netdev=wan,mac=52:54:00:89:00:0$n"
  )
  if [[ "$n" != 4 ]]; then
    # Cluster NIC: connect to the loopback switch so nodes can reach each other.
    start_switch
    network+=(
      -netdev "socket,id=cluster,connect=127.0.0.1:23890"
      -device "virtio-net-pci,netdev=cluster,mac=52:54:00:89:01:0$n"
    )
  fi
  # Launch QEMU with TCG (software emulation) for x86-64 on Apple Silicon.
  # -daemonize detaches the process; the PID is written to qemu.pid for lifecycle management.
  "$qemu/qemu-system-x86_64" -name "$guest" -machine q35,accel=tcg \
    -cpu max -smp "$cpus" -m "$memory" -display none -monitor none \
    -drive "file=$dir/$guest/root.qcow2,if=virtio,format=qcow2" \
    -drive "file=$dir/$guest/seed.iso,media=cdrom,readonly=on" \
    "${network[@]}" -serial "file:$dir/$guest/serial.log" \
    -pidfile "$dir/$guest/qemu.pid" -daemonize
}

wait_ssh() {
  # Poll SSH every 3 seconds for up to 9 minutes (TCG boot is slow).
  # Once SSH responds, verify the arch (x86_64) and OS version match expectations.
  for ((attempt=1; attempt<=180; attempt++)); do
    if ssh -F "$dir/ssh_config" "$guest" true 2>/dev/null; then
      ssh -F "$dir/ssh_config" "$guest" \
        'cloud-init status --wait && test "$(uname -m)" = x86_64 &&
         . /etc/os-release && test "$VERSION" = "26.04.1 LTS (Resolute Raccoon)" &&
         printf "%s %s %s\n" "$(hostname)" "$(uname -m)" "$PRETTY_NAME"'
      return
    fi
    sleep 3
  done
  printf 'SSH timed out for %s. Inspect %s/%s/serial.log\n' "$guest" "$dir" "$guest" >&2
  return 1
}

stop() {
  if ! running "$guest"; then return; fi
  # Schedule a clean shutdown via systemd 2 seconds after the command returns,
  # so the SSH connection can close before the guest powers off.
  ssh -F "$dir/ssh_config" "$guest" 'sudo systemd-run --on-active=2s systemctl poweroff'
  for ((attempt=1; attempt<=90; attempt++)); do
    if ! running "$guest"; then return; fi
    sleep 2
  done
  printf 'Shutdown timed out for %s\n' "$guest" >&2
  return 1
}

action=${1:?Usage: vms.sh prepare | start/wait/stop/status cosi-N ...}
shift
if [[ "$action" == prepare ]]; then
  prepare
  exit
fi
test "$#" -gt 0
for guest in "$@"; do
  case "$guest" in cosi-[1-4]) n=${guest#cosi-} ;; *) exit 2 ;; esac
  case "$action" in
    start) start ;;
    wait) wait_ssh ;;
    stop) stop ;;
    status) if running "$guest"; then state=running; else state=stopped; fi
            printf '%s %s\n' "$guest" "$state" ;;
    *) exit 2 ;;
  esac
done
