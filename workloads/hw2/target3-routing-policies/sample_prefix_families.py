#!/usr/bin/env python3
"""Build a reproducible, scaled prefix workload from Mooncake hash IDs."""

import argparse
import hashlib
import json
import random
from collections import Counter, defaultdict
from pathlib import Path


def stable_route(prefix: tuple[int, ...], backends: int) -> int:
    encoded = ",".join(str(value) for value in prefix).encode("ascii")
    digest = hashlib.sha256(encoded).digest()
    return int.from_bytes(digest[:8], "big") % backends


def source_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_groups(
    path: Path,
    shared_blocks: int,
    output_min: int,
    output_max: int,
) -> dict[tuple[int, ...], list[dict]]:
    groups = defaultdict(list)
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            record = json.loads(line)
            hashes = tuple(int(value) for value in record.get("hash_ids", []))
            output_length = int(record["output_length"])
            if len(hashes) < shared_blocks or not output_min <= output_length <= output_max:
                continue
            record["source_line"] = line_number
            record["hash_ids"] = hashes
            groups[hashes[:shared_blocks]].append(record)
    return groups


def take_round_robin(
    available: dict[int, list[tuple[int, ...]]],
    count: int,
    start_route: int = 0,
) -> list[tuple[int, ...]]:
    selected = []
    routes = sorted(available)
    cursor = start_route % len(routes)
    while len(selected) < count:
        route = routes[cursor]
        if available[route]:
            selected.append(available[route].pop())
        cursor = (cursor + 1) % len(routes)
        if not any(available.values()) and len(selected) < count:
            raise ValueError(f"not enough families to select {count} entries")
    return selected


def distribute(total: int, families: list[tuple[int, ...]]) -> dict[tuple[int, ...], int]:
    if total < len(families):
        raise ValueError("request total must be at least the number of families")
    base, extra = divmod(total, len(families))
    return {
        prefix: base + (1 if index < extra else 0)
        for index, prefix in enumerate(families)
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--backends", type=int, default=4)
    parser.add_argument("--families-per-backend", type=int, default=16)
    parser.add_argument("--measured-requests", type=int, default=2048)
    parser.add_argument("--superhot-families", type=int, default=1)
    parser.add_argument("--superhot-requests", type=int, default=768)
    parser.add_argument("--hot-families", type=int, default=7)
    parser.add_argument("--hot-requests", type=int, default=560)
    parser.add_argument("--warm-families", type=int, default=24)
    parser.add_argument("--warm-requests", type=int, default=480)
    parser.add_argument("--shared-blocks", type=int, default=8)
    parser.add_argument("--max-blocks", type=int, default=12)
    parser.add_argument("--tokens-per-block", type=int, default=128)
    parser.add_argument("--suffix-tokens", type=int, default=64)
    parser.add_argument("--output-min", type=int, default=16)
    parser.add_argument("--output-max", type=int, default=190)
    args = parser.parse_args()

    if args.backends < 2 or args.families_per_backend < 1:
        raise ValueError("the workload needs at least two backends and one family per backend")
    if args.max_blocks < args.shared_blocks:
        raise ValueError("--max-blocks must be at least --shared-blocks")

    selected_family_count = args.backends * args.families_per_backend
    named_family_count = (
        args.superhot_families + args.hot_families + args.warm_families
    )
    if named_family_count >= selected_family_count:
        raise ValueError("the popularity profile must leave at least one cold family")

    named_requests = args.superhot_requests + args.hot_requests + args.warm_requests
    cold_requests = args.measured_requests - named_requests
    cold_family_count = selected_family_count - named_family_count
    if cold_requests < cold_family_count:
        raise ValueError("--measured-requests leaves too few requests for cold families")

    groups = load_groups(
        args.trace,
        args.shared_blocks,
        args.output_min,
        args.output_max,
    )
    eligible = {prefix: records for prefix, records in groups.items() if len(records) >= 2}
    by_route = {
        route: sorted(
            (prefix for prefix in eligible if stable_route(prefix, args.backends) == route),
            key=lambda prefix: tuple(prefix),
        )
        for route in range(args.backends)
    }
    for route, prefixes in by_route.items():
        if len(prefixes) < args.families_per_backend:
            raise ValueError(
                f"route {route} has {len(prefixes)} eligible families, "
                f"need {args.families_per_backend}"
            )

    rng = random.Random(args.seed)
    chosen_by_route = {
        route: rng.sample(prefixes, args.families_per_backend)
        for route, prefixes in by_route.items()
    }
    for prefixes in chosen_by_route.values():
        rng.shuffle(prefixes)

    available = {route: prefixes.copy() for route, prefixes in chosen_by_route.items()}
    superhot = sorted(
        (prefix for prefixes in available.values() for prefix in prefixes),
        key=lambda prefix: (
            sum(int(record["output_length"]) for record in eligible[prefix])
            / len(eligible[prefix]),
            max(int(record["output_length"]) for record in eligible[prefix]),
            prefix,
        ),
        reverse=True,
    )[: args.superhot_families]
    for prefix in superhot:
        available[stable_route(prefix, args.backends)].remove(prefix)
    hot = take_round_robin(available, args.hot_families, start_route=1)
    warm = take_round_robin(available, args.warm_families, start_route=0)
    cold = [prefix for route in sorted(available) for prefix in available[route]]
    rng.shuffle(cold)

    tiers = {
        "superhot": superhot,
        "hot": hot,
        "warm": warm,
        "cold": cold,
    }
    request_counts = {}
    request_counts.update(distribute(args.superhot_requests, superhot))
    request_counts.update(distribute(args.hot_requests, hot))
    request_counts.update(distribute(args.warm_requests, warm))
    request_counts.update(distribute(cold_requests, cold))

    selected = [*superhot, *hot, *warm, *cold]
    tier_by_prefix = {
        prefix: tier for tier, prefixes in tiers.items() for prefix in prefixes
    }
    warmups = []
    measured = []
    for family_index, prefix in enumerate(selected):
        digest = hashlib.sha256(",".join(map(str, prefix)).encode("ascii")).hexdigest()
        family_id = digest[:12]
        family_rng = random.Random(args.seed ^ int(digest[:16], 16))
        templates = eligible[prefix].copy()
        family_rng.shuffle(templates)
        warmup_source = templates[0]
        measured_sources = templates[1:]
        route = stable_route(prefix, args.backends)

        def base_record(source: dict, member_index: int) -> dict:
            hashes = list(source["hash_ids"][: args.max_blocks])
            return {
                "prefix_family": family_id,
                "popularity_tier": tier_by_prefix[prefix],
                "family_order": family_index,
                "family_member": member_index,
                "family_request_count": request_counts[prefix],
                "affinity_backend": route,
                "source_line": source["source_line"],
                "trace_timestamp_ms": int(source["timestamp"]),
                "trace_input_tokens": int(source["input_length"]),
                "trace_output_tokens": int(source["output_length"]),
                "hash_ids": hashes,
                "shared_prefix_blocks": args.shared_blocks,
                "tokens_per_block": args.tokens_per_block,
                "suffix_tokens": args.suffix_tokens,
                "input_tokens": len(hashes) * args.tokens_per_block
                + args.suffix_tokens,
            }

        warmup = base_record(warmup_source, 0)
        warmup.update(
            {
                "request_id": f"warmup-{family_index:03d}",
                "phase": "warmup",
                "output_tokens": 1,
            }
        )
        warmups.append(warmup)

        for replay_index in range(request_counts[prefix]):
            source = measured_sources[replay_index % len(measured_sources)]
            record = base_record(source, replay_index + 1)
            record.update(
                {
                    "request_id": None,
                    "phase": "measured",
                    "replay_index": replay_index,
                    "output_tokens": int(source["output_length"]),
                }
            )
            measured.append(record)

    rng.shuffle(measured)
    for request_id, record in enumerate(measured):
        record["request_id"] = request_id

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as file:
        for record in [*warmups, *measured]:
            file.write(json.dumps(record, ensure_ascii=True) + "\n")

    affinity_requests = Counter(
        record["affinity_backend"] for record in measured
    )
    print(f"source_sha256={source_sha256(args.trace)}")
    print(f"eligible_families={len(eligible)}")
    print(f"selected_families={len(selected)}")
    print(f"warmup_requests={len(warmups)}")
    print(f"measured_requests={len(measured)}")
    print("tier_families=" + json.dumps({k: len(v) for k, v in tiers.items()}))
    print(
        "tier_requests="
        + json.dumps(
            Counter(record["popularity_tier"] for record in measured),
            sort_keys=True,
        )
    )
    print(
        "affinity_family_distribution="
        + json.dumps(
            Counter(stable_route(prefix, args.backends) for prefix in selected),
            sort_keys=True,
        )
    )
    print(
        "affinity_request_distribution="
        + json.dumps(affinity_requests, sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
