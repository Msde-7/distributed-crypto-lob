#!/bin/bash
# Security baseline for every VM. Idempotent. Safe to re-run.
# - Patches OS, enables auto-security-updates
# - SSH: key-only, no root, no password fallback
# - fail2ban for SSH brute-force protection
# - UFW: deny all inbound except SSH from anywhere and any port from project subnet 10.4.36.0/24

set -euo pipefail

echo "[1/6] apt update + security upgrade"
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq update
sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq upgrade

echo "[2/6] install ufw, fail2ban, unattended-upgrades"
sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install ufw fail2ban unattended-upgrades

echo "[3/6] enable unattended security upgrades"
sudo tee /etc/apt/apt.conf.d/20auto-upgrades >/dev/null <<'EOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF

echo "[4/6] sshd: key-only, no root, no challenge-response"
sudo sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo sed -i 's/^#*PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
sudo sed -i 's/^#*ChallengeResponseAuthentication.*/ChallengeResponseAuthentication no/' /etc/ssh/sshd_config
# Ubuntu 24.04 also has 50-cloud-init.conf which can override the above
if [ -f /etc/ssh/sshd_config.d/50-cloud-init.conf ]; then
  sudo sed -i 's/^PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config.d/50-cloud-init.conf
fi
sudo systemctl restart ssh

echo "[5/6] enable fail2ban"
sudo systemctl enable --now fail2ban >/dev/null 2>&1 || true

echo "[6/6] ufw: allow ssh from anywhere + any port from same project subnet (10.4.36.0/24)"
sudo ufw default deny incoming >/dev/null
sudo ufw default allow outgoing >/dev/null
sudo ufw allow ssh >/dev/null
sudo ufw allow from 10.4.36.0/24 >/dev/null
sudo ufw --force enable >/dev/null

echo "==> verification:"
sudo sshd -T 2>/dev/null | grep -E '^(passwordauth|permitrootlogin|challengeresponse)'
sudo systemctl is-active fail2ban || true
sudo ufw status verbose | head -20
echo "==> bootstrap done on $(hostname)"
