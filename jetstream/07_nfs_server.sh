#!/bin/bash
# Run on driver01.
# Export /data over NFS so executors can write Parquet to it as if local.

set -euo pipefail

EXEC_SUBNET="10.4.36.0/24"

sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install nfs-kernel-server

# Permit RW from any VM in the project subnet. no_root_squash so the spark
# container (running as uid 185 / "spark") can create files.
sudo tee /etc/exports >/dev/null <<EOF
/data ${EXEC_SUBNET}(rw,sync,no_subtree_check,no_root_squash)
EOF

sudo systemctl enable --now nfs-kernel-server
sudo exportfs -ra

# UFW: allow NFS (2049) from project subnet only
sudo ufw allow from ${EXEC_SUBNET} to any port 2049 proto tcp

echo "==> exports:"
sudo exportfs -v
echo "==> NFS server up on driver01"
