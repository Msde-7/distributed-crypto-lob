#!/bin/bash
# Run on a fresh m3.small to convert it into a Spark worker.
# Idempotent: chains 01_harden + 08_nfs_client + 04_spark_worker.
set -uo pipefail

DRIVER_IP=10.4.36.243
MASTER_URL="spark://${DRIVER_IP}:7077"
INTERNAL_IP=$(hostname -I | awk '{print $1}')

echo "=================================================="
echo "==> bootstrapping $(hostname) (${INTERNAL_IP}) as Spark executor"
echo "=================================================="

# 1. security baseline (idempotent)
echo
echo "==> [1/3] security baseline"
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq update
sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install ufw fail2ban unattended-upgrades nfs-common
sudo tee /etc/apt/apt.conf.d/20auto-upgrades >/dev/null <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF
sudo sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo sed -i 's/^#*PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
[ -f /etc/ssh/sshd_config.d/50-cloud-init.conf ] && \
  sudo sed -i 's/^PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config.d/50-cloud-init.conf
sudo systemctl restart ssh
sudo systemctl enable --now fail2ban >/dev/null 2>&1 || true
sudo ufw default deny incoming >/dev/null
sudo ufw default allow outgoing >/dev/null
sudo ufw allow ssh >/dev/null
sudo ufw allow from 10.4.36.0/24 >/dev/null
sudo ufw --force enable >/dev/null

# 2. NFS mount of driver01:/data
echo
echo "==> [2/3] mount driver01:/data via NFS"
sudo mkdir -p /data
sudo umount /data 2>/dev/null || true
sudo mount -t nfs ${DRIVER_IP}:/data /data
if ! grep -q "${DRIVER_IP}:/data" /etc/fstab; then
    echo "${DRIVER_IP}:/data /data nfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
fi
mount | grep /data || { echo "FAIL: NFS not mounted"; exit 1; }

# 3. Spark worker container, registers with master automatically
echo
echo "==> [3/3] launch spark-worker container"
docker rm -f spark-worker 2>/dev/null || true
docker run -d --name spark-worker --restart unless-stopped \
    --network host \
    -v /data:/data \
    -e SPARK_NO_DAEMONIZE=1 \
    apache/spark:3.5.3-python3 \
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --host ${INTERNAL_IP} \
        --webui-port 8081 \
        --cores 2 \
        --memory 4G \
        ${MASTER_URL}

# wait until registered
for i in $(seq 1 30); do
    if docker logs spark-worker 2>&1 | grep -q "Successfully registered with master"; then
        echo "==> registered with master after ${i}x2s"
        exit 0
    fi
    sleep 2
done
echo "FAIL: worker did not register within 60s"
docker logs --tail 30 spark-worker
exit 1
