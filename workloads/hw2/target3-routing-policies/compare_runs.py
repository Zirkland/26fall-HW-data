#!/usr/bin/env python3
"""Aggregate repeated Ray Serve workload runs and compare configurations."""

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


FIXED_WORKLOAD_SHA256 = (
    "f154ba954e28ca9612257edf5deb0d5dfc6bf27fd0115a701e1f49e52df75b03"
)
CONTROLLED_FIELDS = (
    "model",
    "workload_sha256",
    "max_in_flight",
    "token_base",
    "token_span",
    "token_salt",
    "expected_replicas",
    "expected_backends",
    "expected_nodes",
)


def load_named(raw: str) -> tuple[str, Path, dict]:
    if "=" not in raw:
        raise ValueError("--run must use NAME=SUMMARY_JSON")
    name, path_raw = raw.split("=", 1)
    name = name.strip()
    if not name:
        raise ValueError("--run names cannot be empty")
    path = Path(path_raw)
    with path.open("r", encoding="utf-8") as file:
        return name, path, json.load(file)


def range_stats(values: list[float | int]) -> dict:
    if not values:
        raise ValueError("cannot summarize an empty metric")
    return {
        "values": values,
        "mean": statistics.fmean(values),
        "min": min(values),
        "max": max(values),
    }


def imbalance(distribution: dict, expected_backends: int) -> float:
    values = [float(value) for value in distribution.values()]
    if expected_backends > len(values):
        values.extend(0.0 for _ in range(expected_backends - len(values)))
    if not values or sum(values) == 0:
        return 0.0
    return (max(values) - min(values)) / (sum(values) / len(values))


def nested(run: dict, *keys: str) -> float | int:
    value = run
    for key in keys:
        value = value[key]
    if value is None:
        raise ValueError(f"metric {'.'.join(keys)} is null")
    return value


def valid_run(run: dict) -> bool:
    return not any(
        (
            run.get("failed", 0),
            run.get("incomplete", 0),
            run.get("warmup_failed", 0),
            run.get("warmup_incomplete", 0),
            run.get("validation_errors", []),
        )
    )


def validate_run(
    run: dict,
    path: Path,
    expected_requests: int,
    expected_workload_sha256: str,
) -> None:
    if not valid_run(run):
        raise ValueError(f"run {path} contains failures, incomplete output, or errors")
    if run.get("requests") != expected_requests:
        raise ValueError(
            f"run {path} contains {run.get('requests')} measured requests; "
            f"expected {expected_requests}"
        )
    if run.get("successful") != expected_requests:
        raise ValueError(
            f"run {path} contains {run.get('successful')} successful requests; "
            f"expected {expected_requests}"
        )
    if run.get("workload_sha256") != expected_workload_sha256:
        raise ValueError(
            f"run {path} used workload SHA-256 {run.get('workload_sha256')}; "
            f"expected {expected_workload_sha256}"
        )


def relative_stats(
    current: list[float],
    baseline: list[float],
    operation,
    same_configuration: bool,
    require_nonzero_baseline: bool = False,
) -> dict:
    if same_configuration:
        return range_stats([0.0])
    baseline_mean = statistics.fmean(baseline)
    if require_nonzero_baseline and baseline_mean == 0:
        raise ValueError("cannot compute a relative change from a zero baseline")
    values = [operation(value, baseline_mean) for value in current]
    return range_stats(values)


def build_comparison(
    entries: list[tuple[str, Path, dict]],
    baseline_name: str | None,
    expected_backends: int,
    min_repeats: int,
    expected_requests: int,
    expected_workload_sha256: str,
) -> dict:
    grouped = defaultdict(list)
    for name, path, summary in entries:
        grouped[name].append({"path": str(path), "summary": summary})

    selected_baseline = baseline_name or next(iter(grouped))
    if selected_baseline not in grouped:
        raise ValueError(f"unknown baseline: {selected_baseline}")

    for name, runs in grouped.items():
        if len(runs) < min_repeats:
            raise ValueError(
                f"configuration {name!r} has {len(runs)} run(s), need {min_repeats}"
            )
        for run in runs:
            validate_run(
                run["summary"],
                Path(run["path"]),
                expected_requests,
                expected_workload_sha256,
            )
        signatures = {
            (run["summary"].get("router_name"), run["summary"].get("max_ongoing_requests"))
            for run in runs
        }
        if len(signatures) != 1 or None in next(iter(signatures)):
            raise ValueError(
                f"configuration {name!r} mixes router settings or lacks metadata"
            )

    all_summaries = [run["summary"] for runs in grouped.values() for run in runs]
    controlled_configuration = {}
    for field in CONTROLLED_FIELDS:
        values = {run.get(field) for run in all_summaries}
        if len(values) != 1 or None in values:
            raise ValueError(f"runs do not share one recorded {field} value")
        controlled_configuration[field] = next(iter(values))

    metric_paths = {
        "successful": ("successful",),
        "throughput_rps": ("throughput_rps",),
        "cache_hit_rate": ("cache_hit_rate",),
        "computed_prefill_tokens_total": ("computed_prefill_tokens_total",),
        "ttft_p50_s": ("ttft_s", "p50"),
        "ttft_p95_s": ("ttft_s", "p95"),
        "ttft_p99_s": ("ttft_s", "p99"),
        "latency_p95_s": ("latency_s", "p95"),
        "client_queue_p95_s": ("client_queue_s", "p95"),
        "dispatch_lag_p95_s": ("dispatch_lag_s", "p95"),
    }

    tables = {}
    raw_metrics = {}
    for name, items in grouped.items():
        summaries = [item["summary"] for item in items]
        metrics = {
            metric: [nested(run, *path) for run in summaries]
            for metric, path in metric_paths.items()
        }
        metrics["backend_request_imbalance"] = [
            imbalance(run["backend_distribution"], expected_backends)
            for run in summaries
        ]
        raw_metrics[name] = metrics
        tables[name] = {
            "repeats": len(summaries),
            "valid_repeats": sum(valid_run(run) for run in summaries),
            **{metric: range_stats(values) for metric, values in metrics.items()},
            "backend_distributions": [
                run["backend_distribution"] for run in summaries
            ],
        }

    baseline_metrics = raw_metrics[selected_baseline]
    for name, table in tables.items():
        metrics = raw_metrics[name]
        same = name == selected_baseline
        table["relative_to_baseline"] = {
            "throughput_gain": relative_stats(
                metrics["throughput_rps"],
                baseline_metrics["throughput_rps"],
                lambda value, base: value / base - 1.0,
                same,
                True,
            ),
            "ttft_p95_reduction": relative_stats(
                metrics["ttft_p95_s"],
                baseline_metrics["ttft_p95_s"],
                lambda value, base: 1.0 - value / base,
                same,
                True,
            ),
            "latency_p95_reduction": relative_stats(
                metrics["latency_p95_s"],
                baseline_metrics["latency_p95_s"],
                lambda value, base: 1.0 - value / base,
                same,
                True,
            ),
            "cache_hit_rate_delta": relative_stats(
                metrics["cache_hit_rate"],
                baseline_metrics["cache_hit_rate"],
                lambda value, base: value - base,
                same,
            ),
        }

    return {
        "schema_version": 2,
        "baseline": selected_baseline,
        "expected_backends": expected_backends,
        "expected_requests": expected_requests,
        "expected_workload_sha256": expected_workload_sha256,
        "controlled_configuration": controlled_configuration,
        "runs": dict(grouped),
        "table": tables,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare configurations; repeat --run with the same NAME to aggregate "
            "multiple runs."
        )
    )
    parser.add_argument("--run", action="append", required=True)
    parser.add_argument("--baseline", help="configuration used for relative changes")
    parser.add_argument("--expected-backends", type=int, default=4)
    parser.add_argument("--expected-requests", type=int, default=2048)
    parser.add_argument(
        "--expected-workload-sha256",
        default=FIXED_WORKLOAD_SHA256,
    )
    parser.add_argument("--min-repeats", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if any(
        value <= 0
        for value in (
            args.expected_backends,
            args.expected_requests,
            args.min_repeats,
        )
    ):
        parser.error(
            "--expected-backends, --expected-requests, and --min-repeats "
            "must be positive"
        )

    try:
        entries = [load_named(raw) for raw in args.run]
        result = build_comparison(
            entries,
            args.baseline,
            args.expected_backends,
            args.min_repeats,
            args.expected_requests,
            args.expected_workload_sha256,
        )
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        parser.error(str(exc))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(result, indent=2, ensure_ascii=True) + "\n"
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
