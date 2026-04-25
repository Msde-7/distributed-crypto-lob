#!/bin/bash
# mount driver01:/data on this executor. fstab entry so it survives reboots.
# the test at the bottom is just so we can retreive a clear failure if the
# mount works but writes dont.

set -euo pipefail

DRIVER_IP=10.4.36.243

sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install nfs-common

sudo mkdir -p /data
sudo umount /data 2>/dev/null || true
sudo mount -t nfs ${DRIVER_IP}:/data /data

if ! grep -q "${DRIVER_IP}:/data" /etc/fstab; then
    echo "${DRIVER_IP}:/data /data nfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
fi

echo "==> mounted on $(hostname):"
mount | grep /data
echo "==> writable test:"
touch /data/.nfs-write-test-$(hostname) && rm /data/.nfs-write-test-$(hostname) && echo "OK: writable from $(hostname)"
