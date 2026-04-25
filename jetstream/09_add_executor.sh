#!/bin/bash
# turn a fresh m3.small into a spark worker. wraps the harden + nfs + worker
# steps so a new exec is one ssh away. safe to re-run, the apt + ufw + docker
# bits are all idempotent so the environement ends up the same regardless.

set -uo pipefail

DRIVER_IP=10.4.36.243
MASTER_URL="spark://${DRIVER_IP}:7077"
INTERNAL_IP=$(hostname -I | awk '{print $1}')

echo "=================================================="
echo "==> bootstrapping $(hostname) (${INTERNAL_IP})"
echo "=================================================="

# harden
echo
echo "==> security baseline"
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq update
sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install ufw fail2ban unattended-upgrades nfs-common
sudo tee /etc/apt/apt.conf.d/20auto-upgrades >/dev/null <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF
sudo sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo sed -i 's/^#*PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
# the cloud-init sshd_config drop-in overrides the main config which is wierd
# but normal on jetstream2 ubuntu24 images, patch both
sudo mkdir -p /data
sudo umount /data 2>/dev/null || true
sudo mount -t nfs ${DRIVER_IP}:/data /data
if ! grep -q "${DRIVER_IP}:/data" /etc/fstab; then
    echo "${DRIVER_IP}:/data /data nfs defaults,_netdev 0 0" | sudo tee -a /etc/fstab
fi
mount | grep /data || { echo "FAIL: NFS not mounted"; exit 1; }

# spark worker
echo
echo "==> launch spark-worker container"
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

# wait for register
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
