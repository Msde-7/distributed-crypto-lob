import re
import sys
from pathlib import Path

log_path = Path(sys.argv[1] if len(sys.argv) > 1 else "spark_run.log")
text = log_path.read_text(encoding="utf-8", errors="replace")

rps_vals = [float(x) for x in re.findall(r"batch_records_per_sec=([\d.]+)", text)]
sec_vals = [float(x) for x in re.findall(r"batch_time_sec=([\d.]+)", text)]
rec_vals = [int(x) for x in re.findall(r"batch_records=(\d+)", text)]
p50_vals = [float(x) for x in re.findall(r"latency_p50_ms=([\d.]+)", text)]
p95_vals = [float(x) for x in re.findall(r"latency_p95_ms=([\d.]+)", text)]
p99_vals = [float(x) for x in re.findall(r"latency_p99_ms=([\d.]+)", text)]

total_batches = re.findall(r"total_batches=(\d+)", text)
total_records = re.findall(r"total_records=(\d+)", text)
total_resyncs = re.findall(r"total_resyncs=(\d+)", text)

last_quotes = {}
for key, bid, ask, spread in re.findall(
    r"\[([a-z]+:[A-Z0-9\-]+)\] best_bid=([\w.\-]+) best_ask=([\w.\-]+) spread=([\w.\-]+)",
    text,
):
    last_quotes[key] = (bid, ask, spread)

def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0

mean_rps = mean([r for r in rps_vals if r > 0])
mean_ms = mean(sec_vals) * 1000
peak_rps = max(rps_vals) if rps_vals else 0.0
peak_batch = max(rec_vals) if rec_vals else 0

print("=" * 60)
print(f"log file:              {log_path}")
print(f"batches parsed:        {len(sec_vals)}")
print(f"total_batches (last):  {total_batches[-1] if total_batches else 'n/a'}")
print(f"total_records (last):  {total_records[-1] if total_records else 'n/a'}")
print(f"total_resyncs (last):  {total_resyncs[-1] if total_resyncs else 'n/a'}")
print("-" * 60)
print(f"mean events/sec:       {mean_rps:.2f}")
print(f"mean ms/batch:         {mean_ms:.2f}")
print(f"peak events/sec:       {peak_rps:.2f}")
print(f"peak batch size:       {peak_batch}")
if p50_vals:
    print(f"latency p50 (ms):      {mean(p50_vals):.2f}")
    print(f"latency p95 (ms):      {mean(p95_vals):.2f}")
    print(f"latency p99 (ms):      {mean(p99_vals):.2f}")
    print(f"latency p99 peak (ms): {max(p99_vals):.2f}")
print("-" * 60)
print("last quote per book (compare one of these to a REST call):")
for key, (bid, ask, spread) in last_quotes.items():
    print(f"  {key}: best_bid={bid}  best_ask={ask}  spread={spread}")
print("=" * 60)
print("\nPaste-ready sentence:")
print(
    f"Over {total_batches[-1] if total_batches else '?'} batches we processed "
    f"{total_records[-1] if total_records else '?'} events at a mean of "
    f"{mean_rps:.0f} events/sec and {mean_ms:.0f} ms/batch, with "
    f"{total_resyncs[-1] if total_resyncs else '?'} resyncs."
)
