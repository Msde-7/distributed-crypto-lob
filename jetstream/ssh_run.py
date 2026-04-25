"""ssh harness for the jetstream2 boxes. handles the bastion case for execs that
have no public ip (we tunnel through driver01).

usage:
    python ssh_run.py <vm> <script.sh>
    python ssh_run.py <vm> --cmd "..."
    python ssh_run.py all <script.sh>
"""
import os
import sys
from pathlib import Path

import paramiko


def _load_dotenv():
    # tiny inline parser, dont want a python-dotenv dep just for this
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()

KEY_PATH = os.path.expanduser(os.environ.get("JETSTREAM_KEY_PATH", "~/.ssh/id_rsa"))
KEY_PASSPHRASE = os.environ.get("JETSTREAM_KEY_PASSPHRASE")
if not KEY_PASSPHRASE:
    sys.exit("JETSTREAM_KEY_PASSPHRASE missing. put it in .env (gitignored).")


# via=None means directly reachable. otherwise its the name of a bastion host
# that has to be in HOSTS already and reachable on its own.
HOSTS = {
    "kafka01":  {"ip": "149.165.174.26",  "via": None},
    "driver01": {"ip": "149.165.169.161", "via": None},
    "exec01":   {"ip": "149.165.175.79",  "via": None},
    "exec02":   {"ip": "149.165.172.58",  "via": None},
    # internal-only, no floating ip. go through driver01.
    "exec03":   {"ip": "10.4.36.226",     "via": "driver01"},
    "exec04":   {"ip": "10.4.36.110",     "via": "driver01"},
    "exec05":   {"ip": "10.4.36.20",      "via": "driver01"},
    "exec06":   {"ip": "10.4.36.219",     "via": "driver01"},
    "exec07":   {"ip": "10.4.36.69",      "via": "driver01"},
    "exec08":   {"ip": "10.4.36.92",      "via": "driver01"},
}

ALL_VMS = list(HOSTS.keys())


def _new_key():
    return paramiko.RSAKey.from_private_key_file(KEY_PATH, password=KEY_PASSPHRASE)


def _connect(vm_name):
    info = HOSTS[vm_name]
    via = info["via"]
    if via is None:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        c.connect(info["ip"], username="exouser", pkey=_new_key(),
                  timeout=30, allow_agent=False, look_for_keys=False)
        return c, None
    # ssh to bastion, open a tcp channel to target:22, run ssh over that.
    # paramikos version of ssh -J basically.
    bastion = _connect(via)[0]
    chan = bastion.get_transport().open_channel(
        "direct-tcpip", (info["ip"], 22), ("127.0.0.1", 0))
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(info["ip"], username="exouser", pkey=_new_key(),
              sock=chan, timeout=30, allow_agent=False, look_for_keys=False)
    return c, bastion


def run_script(vm_name, script_text, label=None):
    label = label or vm_name
    print(f"\n========== {label} ({HOSTS[vm_name]['ip']}) ==========", flush=True)
    c, bastion = _connect(vm_name)
    try:
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
        return rc
    finally:
        c.close()
        if bastion is not None:
            bastion.close()


def run_cmd(vm_name, cmd):
    return run_script(vm_name, cmd + "\n")


if __name__ == "__main__":
    # neccessary for windows git bash, otherwise utf-8 in remote stdout breaks
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
