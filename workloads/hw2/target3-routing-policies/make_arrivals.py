#!/usr/bin/env python3
"""Attach a deterministic, optionally multi-phase Poisson schedule."""

import argparse
import json
import random
from pathlib import Path


def parse_profile(raw: str) -> list[tuple[str, int, float]]:
    phases = []
    for item in raw.split(","):
        try:
            name, count, qps = item.split(":")
            phase = (name.strip(), int(count), float(qps))
        except ValueError as exc:
            raise ValueError(
                "--profile must use NAME:COUNT:QPS entries separated by commas"
            ) from exc
        if not phase[0] or phase[1] <= 0 or phase[2] <= 0:
            raise ValueError("profile names must be set and COUNT/QPS must be positive")
        phases.append(phase)
    return phases


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=Path, required=True)
    schedule = parser.add_mutually_exclusive_group(required=True)
    schedule.add_argument("--qps", type=float)
    schedule.add_argument(
        "--profile",
        help="comma-separated NAME:COUNT:QPS entries",
    )
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.qps is not None and args.qps <= 0:
        raise ValueError("--qps must be positive")
    with args.sample.open("r", encoding="utf-8") as file:
        records = [json.loads(line) for line in file if line.strip()]

    measured_total = sum(record["phase"] == "measured" for record in records)
    phases = (
        parse_profile(args.profile)
        if args.profile
        else [("steady", measured_total, args.qps)]
    )
    if sum(count for _, count, _ in phases) != measured_total:
        raise ValueError(
            "profile request counts must equal the number of measured requests "
            f"({measured_total})"
        )

    phase_by_index = []
    for name, count, qps in phases:
        phase_by_index.extend((name, qps, index) for index in range(count))

    rng = random.Random(args.seed)
    arrival = 0.0
    measured_count = 0
    for record in records:
        if record["phase"] == "warmup":
            record["arrival_offset_s"] = None
            continue
        traffic_phase, qps, phase_index = phase_by_index[measured_count]
        if measured_count:
            arrival += rng.expovariate(qps)
        record["arrival_offset_s"] = round(arrival, 6)
        record["arrival_rate_rps"] = qps
        record["arrival_seed"] = args.seed
        record["traffic_phase"] = traffic_phase
        record["traffic_phase_index"] = phase_index
        measured_count += 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=True) + "\n")

    print(
        "profile="
        + ",".join(f"{name}:{count}:{qps:g}" for name, count, qps in phases)
    )
    print(f"measured_requests={measured_count}")
    print(f"planned_duration_s={arrival:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
