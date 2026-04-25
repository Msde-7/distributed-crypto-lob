#!/bin/bash
# nfs server on driver01. exports /data so the executor containers can write
# parquet to it like a local fs. needs root_squash off so the spark container
# (uid 185) gets the right write priviledges.

set -euo pipefail

EXEC_SUBNET="10.4.36.0/24"

sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install nfs-kernel-server

sudo tee /etc/exports >/dev/null <<EOF
/data ${EXEC_SUBNET}(rw,sync,no_subtree_check,no_root_squash)
EOF

sudo systemctl enable --now nfs-kernel-server
sudo exportfs -ra

# 2049 inbound from project subnet only
sudo ufw allow from ${EXEC_SUBNET} to any port 2049 proto tcp

echo "==> exports:"
sudo exportfs -v
echo "==> NFS up on driver01"
