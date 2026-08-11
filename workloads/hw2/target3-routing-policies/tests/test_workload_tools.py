import argparse
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from aiohttp import web

import compare_runs
import run_workload


def summary(throughput: float, ttft: float, cache_hit: float) -> dict:
    return {
        "requests": 2048,
        "successful": 2048,
        "failed": 0,
        "incomplete": 0,
        "warmup_failed": 0,
        "warmup_incomplete": 0,
        "validation_errors": [],
        "router_name": "p2c",
        "max_ongoing_requests": 5,
        "model": "Qwen/Qwen3-0.6B",
        "workload_sha256": compare_runs.FIXED_WORKLOAD_SHA256,
        "max_in_flight": 2048,
        "token_base": 4096,
        "token_span": 60000,
        "token_salt": 2026,
        "expected_replicas": 4,
        "expected_backends": 4,
        "expected_nodes": 4,
        "throughput_rps": throughput,
        "cache_hit_rate": cache_hit,
        "computed_prefill_tokens_total": 100,
        "ttft_s": {"p50": ttft / 2, "p95": ttft, "p99": ttft * 1.2},
        "latency_s": {"p95": ttft * 2},
        "client_queue_s": {"p95": 0.001},
        "dispatch_lag_s": {"p95": 0.002},
        "backend_distribution": {"a": 512, "b": 512, "c": 512, "d": 512},
    }


class CompareRunsTests(unittest.TestCase):
    def test_repeated_names_are_aggregated(self) -> None:
        entries = [
            ("A", Path("a1.json"), summary(10.0, 4.0, 0.5)),
            ("A", Path("a2.json"), summary(12.0, 5.0, 0.6)),
            ("B", Path("b1.json"), summary(20.0, 2.0, 0.7)),
            ("B", Path("b2.json"), summary(22.0, 3.0, 0.8)),
        ]
        result = compare_runs.build_comparison(
            entries,
            "A",
            4,
            2,
            2048,
            compare_runs.FIXED_WORKLOAD_SHA256,
        )

        self.assertEqual(result["table"]["A"]["repeats"], 2)
        self.assertEqual(result["table"]["B"]["throughput_rps"]["min"], 20.0)
        self.assertEqual(result["table"]["B"]["throughput_rps"]["max"], 22.0)
        self.assertEqual(
            result["table"]["A"]["relative_to_baseline"]["throughput_gain"][
                "values"
            ],
            [0.0],
        )

    def test_missing_backends_count_as_zero_load(self) -> None:
        self.assertEqual(compare_runs.imbalance({"a": 10, "b": 10}, 4), 2.0)

    def test_invalid_run_is_rejected(self) -> None:
        invalid = summary(10.0, 4.0, 0.5)
        invalid["failed"] = 1
        with self.assertRaisesRegex(ValueError, "contains failures"):
            compare_runs.build_comparison(
                [("A", Path("a.json"), invalid)],
                "A",
                4,
                1,
                2048,
                compare_runs.FIXED_WORKLOAD_SHA256,
            )

    def test_repeated_name_must_use_one_router_configuration(self) -> None:
        first = summary(10.0, 4.0, 0.5)
        second = summary(11.0, 4.0, 0.5)
        second["max_ongoing_requests"] = 32
        with self.assertRaisesRegex(ValueError, "mixes router settings"):
            compare_runs.build_comparison(
                [
                    ("A", Path("a1.json"), first),
                    ("A", Path("a2.json"), second),
                ],
                "A",
                4,
                2,
                2048,
                compare_runs.FIXED_WORKLOAD_SHA256,
            )

    def test_all_groups_must_share_controlled_conditions(self) -> None:
        first = summary(10.0, 4.0, 0.5)
        second = summary(11.0, 4.0, 0.5)
        second["model"] = "another-model"
        with self.assertRaisesRegex(ValueError, "recorded model"):
            compare_runs.build_comparison(
                [
                    ("A", Path("a.json"), first),
                    ("B", Path("b.json"), second),
                ],
                "A",
                4,
                1,
                2048,
                compare_runs.FIXED_WORKLOAD_SHA256,
            )


class RunWorkloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async def generate(request: web.Request) -> web.Response:
            payload = await request.json()
            output_tokens = int(payload["sampling_params"]["max_new_tokens"])
            event = {
                "output_ids": list(range(output_tokens)),
                "meta_info": {
                    "prompt_tokens": len(payload["input_ids"]),
                    "cached_tokens": 2,
                    "finish_reason": {"type": "length"},
                },
            }
            return web.Response(
                text=f"data: {json.dumps(event)}\n\ndata: [DONE]\n\n",
                content_type="text/event-stream",
                headers={
                    "X-Ray-Replica-ID": "replica-0",
                    "X-Ray-Node-ID": "node-0",
                    "X-SGLang-Backend": "backend-0",
                },
            )

        app = web.Application()
        app.router.add_post("/generate", generate)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        await self.runner.cleanup()

    async def test_end_to_end_outputs_include_config_and_routing(self) -> None:
        records = [
            self.record("warmup-000", "warmup", None, 1),
            self.record(0, "measured", 0.0, 2),
            self.record(1, "measured", 0.001, 2),
        ]
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            workload = root / "workload.jsonl"
            workload.write_text(
                "".join(json.dumps(record) + "\n" for record in records),
                encoding="utf-8",
            )
            output_dir = root / "results"
            args = argparse.Namespace(
                policy="serve",
                run_name="test-run",
                workload=workload,
                output_dir=output_dir,
                base_url=f"http://127.0.0.1:{self.port}",
                max_in_flight=8,
                timeout=10.0,
                token_base=100,
                token_span=100,
                token_salt=2026,
                session_header="X-Session-Id",
                model="test-model",
                router_name="test-router",
                max_ongoing_requests=5,
                expected_replicas=1,
                expected_backends=1,
                expected_nodes=1,
                require_routing_headers=True,
                metadata={"course": "hw2"},
                overwrite=False,
                expected_workload_sha256=None,
            )

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = await run_workload.async_main(args)

            self.assertEqual(exit_code, 0)
            self.assertTrue((output_dir / "config.json").is_file())
            self.assertTrue((output_dir / "warmups.csv").is_file())
            self.assertTrue((output_dir / "requests.csv").is_file())
            result = json.loads((output_dir / "summary.json").read_text())
            self.assertEqual(result["run_name"], "test-run")
            self.assertEqual(result["policy"], "serve")
            self.assertEqual(result["node_distribution"], {"node-0": 2})
            self.assertEqual(result["validation_errors"], [])

    def test_required_routing_headers_are_reported(self) -> None:
        _, _, _, missing = run_workload.routing_metadata({}, require=True)
        self.assertEqual(set(missing), set(run_workload.ROUTING_HEADERS.values()))

    def test_existing_run_artifacts_are_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            output_dir = Path(raw_dir)
            (output_dir / "summary.json").write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "already contains run artifacts"):
                run_workload.prepare_output_dir(output_dir, overwrite=False)

    @staticmethod
    def record(request_id, phase: str, arrival, output_tokens: int) -> dict:
        return {
            "request_id": request_id,
            "phase": phase,
            "prefix_family": "family-0",
            "popularity_tier": "hot",
            "traffic_phase": "steady" if phase == "measured" else "",
            "family_request_count": 2,
            "affinity_backend": 0,
            "source_line": 1,
            "arrival_offset_s": arrival,
            "hash_ids": [1],
            "shared_prefix_blocks": 1,
            "tokens_per_block": 2,
            "suffix_tokens": 1,
            "input_tokens": 3,
            "output_tokens": output_tokens,
        }


if __name__ == "__main__":
    unittest.main()
