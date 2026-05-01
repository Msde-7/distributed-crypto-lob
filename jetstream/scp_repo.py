"""sftp the runtime files to a vm. avoids needing github creds on the box
since the repo is private."""
import os
import sys
from pathlib import Path

import paramiko

sys.path.insert(0, os.path.dirname(__file__))
from ssh_run import KEY_PATH, KEY_PASSPHRASE, HOSTS  # noqa


def push(vm_name, dest="/home/exouser/distributed-crypto-lob"):
    root = Path(__file__).resolve().parent.parent
    files = [
        "normalizer.py", "order_book.py", "resync.py", "partitioning.py",
        "producer_stream.py", "producer_binance.py", "producer_kraken.py",
        "producer_synth.py",
        "spark_order_book.py", "run_experiment.py",
        "parse_run.py", "parse_sweep.py",
        "validate_adapters.py", "probe_live_ws.py", "requirements.txt",
        "Dockerfile.producer", "Dockerfile.spark", "docker-compose.yml",
        "jetstream/demo.py",
    ]
    info = HOSTS[vm_name]
    ip = info["ip"] if isinstance(info, dict) else info
    key = paramiko.RSAKey.from_private_key_file(KEY_PATH, password=KEY_PASSPHRASE)
    t = paramiko.Transport((ip, 22))
    t.connect(username="exouser", pkey=key)
    sftp = paramiko.SFTPClient.from_transport(t)

    # mkdir -p the dest
    parts = dest.strip("/").split("/")
    cur = ""
    for p in parts:
        cur = cur + "/" + p
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            sftp.mkdir(cur)

    pushed = 0
    made_dirs = set()
    for f in files:
        local = root / f
        if not local.exists():
            # supress missing-file errors, we expect a few of the optional ones
            print(f"  skip (missing): {f}")
            continue
        # mkdir any subdir the file lives in (e.g. jetstream/) before put,
        # since sftp.put wont create parent dirs on its own
        sub = "/".join(f.split("/")[:-1])
        if sub and sub not in made_dirs:
            sub_path = f"{dest}/{sub}"
            try:
                sftp.stat(sub_path)
            except FileNotFoundError:
                sftp.mkdir(sub_path)
            made_dirs.add(sub)
        sftp.put(str(local), f"{dest}/{f}")
        pushed += 1
    print(f"==> {vm_name}: pushed {pushed} files to {dest}")
    sftp.close(); t.close()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    push(sys.argv[1] if len(sys.argv) > 1 else "driver01")
