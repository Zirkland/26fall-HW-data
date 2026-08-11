#!/usr/bin/env python3
"""Compare any number of Ray Serve workload summaries."""

import argparse
import json
from pathlib import Path


def load_named(raw: str) -> tuple[str, dict]:
    if "=" not in raw:
        raise ValueError("--run must use NAME=SUMMARY_JSON")
    name, path = raw.split("=", 1)
    with Path(path).open("r", encoding="utf-8") as file:
        return name, json.load(file)


def imbalance(distribution: dict) -> float:
    values = [float(value) for value in distribution.values()]
    if not values or sum(values) == 0:
        return 0.0
    return (max(values) - min(values)) / (sum(values) / len(values))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="append", required=True)
    parser.add_argument("--baseline", help="run name used for relative changes")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    runs = dict(load_named(raw) for raw in args.run)
    baseline_name = args.baseline or next(iter(runs))
    if baseline_name not in runs:
        raise ValueError(f"unknown baseline: {baseline_name}")
    baseline = runs[baseline_name]

    table = {}
    for name, run in runs.items():
        table[name] = {
            "successful": run["successful"],
            "throughput_rps": run["throughput_rps"],
            "cache_hit_rate": run["cache_hit_rate"],
            "computed_prefill_tokens_total": run["computed_prefill_tokens_total"],
            "ttft_p50_s": run["ttft_s"]["p50"],
            "ttft_p95_s": run["ttft_s"]["p95"],
            "ttft_p99_s": run["ttft_s"]["p99"],
            "latency_p95_s": run["latency_s"]["p95"],
            "client_queue_p95_s": run["client_queue_s"]["p95"],
            "backend_request_imbalance": imbalance(run["backend_distribution"]),
            "backend_distribution": run["backend_distribution"],
            "relative_to_baseline": {
                "throughput_gain": (
                    run["throughput_rps"] / baseline["throughput_rps"] - 1.0
                ),
                "ttft_p95_reduction": (
                    1.0 - run["ttft_s"]["p95"] / baseline["ttft_s"]["p95"]
                ),
                "latency_p95_reduction": (
                    1.0
                    - run["latency_s"]["p95"] / baseline["latency_s"]["p95"]
                ),
                "cache_hit_rate_delta": (
                    run["cache_hit_rate"] - baseline["cache_hit_rate"]
                ),
            },
        }

    result = {
        "baseline": baseline_name,
        "runs": runs,
        "table": table,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(result, indent=2, ensure_ascii=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
