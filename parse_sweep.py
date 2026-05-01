"""Aggregate a 2D synth-load sweep into a CSV grid.

reads logs/synth_sweep/N{n}_R{r}.log from 14_synth_sweep.sh, drops warmup
batches, and writes a long-format csv. per cell: p50/p95/p99 latency, actual
rate, max gap_count, sink-error count.

usage: python3 parse_sweep.py logs/synth_sweep [out.csv]
"""
import csv
import re
import sys
from pathlib import Path
from statistics import mean


CELL_RE = re.compile(r"N(\d+)_R(\d+)\.log$")
WARMUP_BATCHES = 5     # drop first N batches as backlog-drain warmup


def parse_one(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")

    p50 = [float(x) for x in re.findall(r"latency_p50_ms=([\d.]+)", text)]
    p95 = [float(x) for x in re.findall(r"latency_p95_ms=([\d.]+)", text)]
    p99 = [float(x) for x in re.findall(r"latency_p99_ms=([\d.]+)", text)]
    rec = [int(x) for x in re.findall(r"total_records=(\d+)", text)]
    bsec = [float(x) for x in re.findall(r"batch_time_sec=([\d.]+)", text)]
    gaps = [int(x) for x in re.findall(r"gap_count=(\d+)", text)]
    sink_errors = len(re.findall(r"FileFormatWriter: Aborting", text))

    if not p99:
        return None

    # drop warmup batches (first WARMUP_BATCHES) so we measure steady state
    p50_s = p50[WARMUP_BATCHES:] if len(p50) > WARMUP_BATCHES else p50
    p95_s = p95[WARMUP_BATCHES:] if len(p95) > WARMUP_BATCHES else p95
    p99_s = p99[WARMUP_BATCHES:] if len(p99) > WARMUP_BATCHES else p99

    # estimate actual rate: total_records grows monotonically; use last - first
    # divided by sum of batch_time_sec (if available) else 1.
    if len(rec) >= 2:
        delta_records = rec[-1] - rec[WARMUP_BATCHES if len(rec) > WARMUP_BATCHES else 0]
        delta_sec = sum(bsec[WARMUP_BATCHES:]) if len(bsec) > WARMUP_BATCHES else sum(bsec)
        actual_rate = delta_records / delta_sec if delta_sec > 0 else 0.0
    else:
        actual_rate = 0.0

    return {
        "p50_ms":      mean(p50_s) if p50_s else float("nan"),
        "p95_ms":      mean(p95_s) if p95_s else float("nan"),
        "p99_ms":      mean(p99_s) if p99_s else float("nan"),
        "actual_rate": actual_rate,
        "max_gaps":    max(gaps) if gaps else 0,
        "sink_errors": sink_errors,
        "batches":     len(p99),
    }


def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(2)

    root = Path(sys.argv[1])
    out_csv = Path(sys.argv[2]) if len(sys.argv) >= 3 else root / "sweep_grid.csv"

    rows = []
    for f in sorted(root.glob("N*_R*.log")):
        m = CELL_RE.search(f.name)
        if not m:
            continue
        n, r = int(m.group(1)), int(m.group(2))
        cell = parse_one(f)
        if cell is None:
            print(f"  SKIP {f.name} (no metrics)", file=sys.stderr)
            continue
        rows.append({"n_workers": n, "target_rate": r, **cell})

    if not rows:
        print("no parseable files found", file=sys.stderr)
        sys.exit(1)

    # write long-format csv
    fieldnames = ["n_workers", "target_rate", "actual_rate",
                  "p50_ms", "p95_ms", "p99_ms",
                  "max_gaps", "sink_errors", "batches"]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in sorted(rows, key=lambda r: (r["target_rate"], r["n_workers"])):
            w.writerow({k: row[k] for k in fieldnames})

    # also print the grid in a easier readable form
    print(f"\nlong-format CSV written to {out_csv}\n")
    print(f"{'rate':>7} | " + " | ".join(f"N={n:>2}" for n in
        sorted(set(r["n_workers"] for r in rows))))
    print("-" * 60)

    by_rate = {}
    for r in rows:
        by_rate.setdefault(r["target_rate"], {})[r["n_workers"]] = r

    n_set = sorted(set(r["n_workers"] for r in rows))
    for rate in sorted(by_rate):
        cells = by_rate[rate]
        line = f"{rate:>7} | "
        line += " | ".join(
            f"{cells[n]['p99_ms']:>5.0f}ms" if n in cells else "  --  "
            for n in n_set
        )
        line += "    p99 ms"
        print(line)
        # second row: actual rate achieved per cell
        line = f"{'':>7} | "
        line += " | ".join(
            f"{cells[n]['actual_rate']/1000:>5.1f}k" if n in cells else "  --  "
            for n in n_set
        )
        line += "    actual rate"
        print(line)
        # third row: gap+error flags
        line = f"{'':>7} | "
        flags = []
        for n in n_set:
            if n in cells:
                c = cells[n]
                flag = "ok"
                if c["max_gaps"] > 0:
                    flag = f"GAP={c['max_gaps']}"
                if c["sink_errors"] > 0:
                    flag = f"SINK_ERR={c['sink_errors']}"
                flags.append(f"{flag:>7}")
            else:
                flags.append("    -- ")
        line += " | ".join(flags)
        line += "    flags"
        print(line)
        print("-" * 60)


if __name__ == "__main__":
    main()
