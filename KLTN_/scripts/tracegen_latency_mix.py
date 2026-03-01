#!/usr/bin/env python3
"""Generate synthetic traces with a controlled fast/slow latency mix.

It exports OTLP spans to an OTEL Collector (gRPC, default localhost:4317).

Each trace contains:
- One root span: "test.request"
- N child spans: "test.work" (sequential)
- Attributes on every span:
  - test.run_id, test.case (fast|slow), test.target_latency_ms, test.span_count

Errors:
- When a trace is marked error, root span status is set to ERROR and a
  http.status_code=500 attribute is added.

Usage:
  /home/leeduc/miniconda3/bin/python scripts/tracegen_latency_mix.py --endpoint localhost:4317 --insecure \
    --seconds 60 --rps 5 --slow-ratio 0.30 --fast-ms 5 40 --slow-ms 800 2500 --spans 10 40 --error-ratio 0.02
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Tuple

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode


@dataclass(frozen=True)
class LatencyProfile:
    fast_ms: Tuple[int, int]
    slow_ms: Tuple[int, int]
    slow_ratio: float


def _now_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S")


def _parse_pair(name: str, values: list[int]) -> Tuple[int, int]:
    if len(values) != 2:
        raise SystemExit(f"{name} expects 2 integers: MIN MAX")
    a, b = int(values[0]), int(values[1])
    if a < 0 or b < 0 or b < a:
        raise SystemExit(f"{name} invalid range: {a}..{b}")
    return a, b


def _make_tracer(*, service_name: str, endpoint: str, insecure: bool) -> None:
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


def _is_error(rng: random.Random, error_ratio: float) -> bool:
    if error_ratio <= 0:
        return False
    if error_ratio >= 1:
        return True
    return rng.random() < error_ratio


def _pick_case_and_latency_ms(rng: random.Random, profile: LatencyProfile) -> Tuple[str, int]:
    slow = rng.random() < max(0.0, min(1.0, profile.slow_ratio))
    lo, hi = profile.slow_ms if slow else profile.fast_ms
    target = rng.randint(lo, hi) if hi > lo else lo
    return ("slow" if slow else "fast"), int(target)


async def _sleep_ms(ms: int) -> None:
    if ms <= 0:
        return
    await asyncio.sleep(ms / 1000.0)


async def _emit_one_trace(
    *,
    rng: random.Random,
    run_id: str,
    profile: LatencyProfile,
    spans_range: Tuple[int, int],
    error_ratio: float,
) -> None:
    tracer = trace.get_tracer("latency-mix")

    test_case, target_latency_ms = _pick_case_and_latency_ms(rng, profile)
    span_count = rng.randint(spans_range[0], spans_range[1]) if spans_range[1] > spans_range[0] else spans_range[0]
    span_count = max(1, int(span_count))

    mark_error = _is_error(rng, error_ratio)

    child_count = max(1, span_count - 1)
    base_ms = target_latency_ms // child_count
    remainder = target_latency_ms - (base_ms * child_count)

    common_attrs = {
        "test.run_id": run_id,
        "test.case": test_case,
        "test.target_latency_ms": target_latency_ms,
        "test.span_count": span_count,
    }

    with tracer.start_as_current_span("test.request", attributes=common_attrs) as root:
        if mark_error:
            root.set_status(Status(StatusCode.ERROR, description="synthetic error"))
            root.set_attribute("http.status_code", 500)

        for i in range(child_count):
            child_ms = base_ms + (1 if i < remainder else 0)
            with tracer.start_as_current_span(
                "test.work",
                attributes={
                    **common_attrs,
                    "test.work.index": i,
                    "test.work.sleep_ms": child_ms,
                },
            ) as child:
                if mark_error and i == child_count - 1:
                    child.set_status(Status(StatusCode.ERROR, description="synthetic error"))
                await _sleep_ms(child_ms)


async def run(load_seconds: int, rps: float, task_concurrency: int, emit_fn) -> None:
    if load_seconds <= 0:
        return
    if rps <= 0:
        raise SystemExit("--rps must be > 0")

    sem = asyncio.Semaphore(max(1, task_concurrency))

    async def guarded_emit() -> None:
        async with sem:
            await emit_fn()

    start = time.time()
    period = 1.0 / rps
    next_t = start

    tasks = []
    while True:
        now = time.time()
        if now - start >= load_seconds:
            break
        tasks.append(asyncio.create_task(guarded_emit()))
        next_t += period
        sleep_s = next_t - time.time()
        if sleep_s > 0:
            await asyncio.sleep(sleep_s)

    if tasks:
        await asyncio.gather(*tasks)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="localhost:4317", help="OTLP gRPC endpoint, e.g. localhost:4317")
    ap.add_argument("--insecure", action="store_true", help="Use insecure gRPC (no TLS)")
    ap.add_argument("--service-name", default="latency-mix-gen", help="Resource service.name")

    ap.add_argument("--run-id", default=None, help="Run id attribute. Default is UTC timestamp")
    ap.add_argument("--seed", type=int, default=1, help="RNG seed")

    ap.add_argument("--seconds", type=int, default=60, help="How long to generate traces")
    ap.add_argument("--rps", type=float, default=5.0, help="Traces per second")
    ap.add_argument("--concurrency", type=int, default=50, help="Max concurrent in-flight traces")

    ap.add_argument("--slow-ratio", type=float, default=0.2, help="Probability that a trace is slow")
    ap.add_argument("--fast-ms", nargs=2, type=int, default=[5, 40], help="Fast latency range (ms): MIN MAX")
    ap.add_argument("--slow-ms", nargs=2, type=int, default=[800, 2500], help="Slow latency range (ms): MIN MAX")
    ap.add_argument("--spans", nargs=2, type=int, default=[10, 40], help="Span count range per trace: MIN MAX")
    ap.add_argument("--error-ratio", type=float, default=0.02, help="Probability that a trace is marked error")

    args = ap.parse_args()

    fast = _parse_pair("--fast-ms", args.fast_ms)
    slow = _parse_pair("--slow-ms", args.slow_ms)
    spans_range = _parse_pair("--spans", args.spans)

    run_id = args.run_id or _now_run_id()
    rng = random.Random(int(args.seed))

    _make_tracer(service_name=args.service_name, endpoint=args.endpoint, insecure=bool(args.insecure))

    profile = LatencyProfile(fast_ms=fast, slow_ms=slow, slow_ratio=float(args.slow_ratio))

    async def emit() -> None:
        await _emit_one_trace(
            rng=rng,
            run_id=run_id,
            profile=profile,
            spans_range=spans_range,
            error_ratio=float(args.error_ratio),
        )

    asyncio.run(run(args.seconds, args.rps, args.concurrency, emit))

    tp = trace.get_tracer_provider()
    if hasattr(tp, "force_flush"):
        tp.force_flush()
    if hasattr(tp, "shutdown"):
        tp.shutdown()

    print(f"done. run_id={run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
