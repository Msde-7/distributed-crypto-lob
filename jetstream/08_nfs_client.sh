#!/bin/bash
# Run on each executor (exec01, exec02).
# Mount driver01:/data at /data so Spark writes land in the shared volume.

set -euo pipefail

DRIVER_IP=10.4.36.243

sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install nfs-common

sudo mkdir -p /data
# unmount if previously mounted (idempotent re-run)
sudo umount /data 2>/dev/null || true

sudo mount -t nfs ${DRIVER_IP}:/data /data

# persist across reboots
if ! grep -q "${DRIVER_IP}:/data" /etc/fstab; then
    echo "${DRIVER_IP}:/data /data nfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
fi

echo "==> mounted on $(hostname):"
mount | grep /data
echo "==> writable test:"
touch /data/.nfs-write-test-$(hostname) && rm /data/.nfs-write-test-$(hostname) && echo "OK: writable from $(hostname)"
