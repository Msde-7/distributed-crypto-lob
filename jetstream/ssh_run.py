"""Tiny SSH harness for running scripts on the four Jetstream2 VMs.

Usage:
    python ssh_run.py <vm-name> <local-script-path>
    python ssh_run.py <vm-name> --cmd "uname -a"
    python ssh_run.py all <local-script-path>
"""
import os
import sys
from pathlib import Path

import paramiko


def _load_dotenv():
    """Load .env (gitignored) into os.environ if present. Tiny inline parser
    so we don't depend on python-dotenv."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


_load_dotenv()

KEY_PATH = os.path.expanduser(os.environ.get("JETSTREAM_KEY_PATH", "~/.ssh/id_rsa"))
KEY_PASSPHRASE = os.environ.get("JETSTREAM_KEY_PASSPHRASE")
if not KEY_PASSPHRASE:
    sys.exit("JETSTREAM_KEY_PASSPHRASE not set. Add it to .env (gitignored).")

HOSTS = {
    "kafka01": "149.165.174.26",
    "driver01": "149.165.169.161",
    "exec01": "149.165.175.79",
    "exec02": "149.165.172.58",
}

ALL_VMS = list(HOSTS.keys())


def _client(ip):
    key = paramiko.RSAKey.from_private_key_file(KEY_PATH, password=KEY_PASSPHRASE)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(ip, username="exouser", pkey=key, timeout=30,
              allow_agent=False, look_for_keys=False)
    return c


def run_script(vm_name, script_text, label=None):
    ip = HOSTS[vm_name]
    label = label or vm_name
    print(f"\n========== {label} ({ip}) ==========", flush=True)
    c = _client(ip)
    stdin, stdout, stderr = c.exec_command("bash -s", get_pty=False)
    stdin.write(script_text)
    stdin.channel.shutdown_write()
    out = stdout.read().decode("utf-8", "replace")
    err = stderr.read().decode("utf-8", "replace")
    rc = stdout.channel.recv_exit_status()
    if out.strip():
        print(out, flush=True)
    if err.strip():
        print(f"[stderr]\n{err}", flush=True)
    print(f"[exit {rc}]", flush=True)
    c.close()
    return rc


def run_cmd(vm_name, cmd):
    return run_script(vm_name, cmd + "\n")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if len(sys.argv) < 3:
        print(__doc__); sys.exit(2)
    target = sys.argv[1]
    if sys.argv[2] == "--cmd":
        script = sys.argv[3] + "\n"
    else:
        script = open(sys.argv[2], encoding="utf-8").read()
    vms = ALL_VMS if target == "all" else [target]
    rcs = [run_script(v, script) for v in vms]
    sys.exit(max(rcs))
