#!/usr/bin/env python3
"""Prove critical-endpoint retention from baseline vs sampled OTLP JSON exports.

Input format: line-delimited OTLP JSON produced by fileexporter.

Outputs:
- coverage of endpoint labels (http.route/http.target)
- retention of critical vs non-critical endpoint traces
- per-endpoint retention table

Example:
  python3 scripts/prove_critical_endpoint_retention.py \
    --baseline signoz/deploy/docker/otel-export/baseline2/20260310-220555-traces.json \
    --sampled  signoz/deploy/docker/otel-export/model1/20260310-220708-traces.model.json \
    --run-id tracka50k-run1
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set


DEFAULT_CRITICAL = [
    "/checkout",
    "/api/checkout",
    "/orders",
    "/api/orders",
    "/payment",
    "/api/payment",
    "/pay",
    "/api/pay",
]


def _iter_records(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _hex_trace_id(any_val: Any) -> Optional[str]:
    if any_val is None:
        return None
    if isinstance(any_val, str):
        return any_val.lower()
    if isinstance(any_val, list):
        try:
            return bytes(int(x) & 0xFF for x in any_val).hex()
        except Exception:
            return None
    return None


def _attrs_from(span: dict) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    attrs = span.get("attributes")

    kvs: Optional[list]
    if isinstance(attrs, list):
        kvs = attrs
    elif isinstance(attrs, dict):
        maybe = attrs.get("keyValues")
        kvs = maybe if isinstance(maybe, list) else None
    else:
        kvs = None

    if not kvs:
        return out

    for kv in kvs:
        if not isinstance(kv, dict):
            continue
        key = kv.get("key")
        val = kv.get("value")
        if not isinstance(key, str) or not isinstance(val, dict):
            continue
        for field in ("stringValue", "intValue", "doubleValue", "boolValue", "bytesValue"):
            if field in val:
                out[key] = val[field]
                break
    return out


@dataclass
class TraceInfo:
    trace_id: str
    endpoint: Optional[str] = None


def _extract_trace_info(path: Path, run_id: Optional[str], endpoint_keys: List[str]) -> Dict[str, TraceInfo]:
    traces: Dict[str, TraceInfo] = {}
    for rec in _iter_records(path):
        rs = rec.get("resourceSpans")
        if not isinstance(rs, list):
            continue
        for r in rs:
            scopes = r.get("scopeSpans")
            if not isinstance(scopes, list):
                continue
            for s in scopes:
                spans = s.get("spans")
                if not isinstance(spans, list):
                    continue
                for sp in spans:
                    tid = _hex_trace_id(sp.get("traceId"))
                    if not tid:
                        continue
                    attrs = _attrs_from(sp)
                    if run_id is not None and attrs.get("test.run_id") != run_id:
                        continue
                    info = traces.get(tid)
                    if info is None:
                        info = TraceInfo(trace_id=tid)
                        traces[tid] = info
                    if info.endpoint is None:
                        for key in endpoint_keys:
                            val = attrs.get(key)
                            if isinstance(val, str) and val:
                                info.endpoint = val
                                break
    return traces


def _pct(n: int, d: int) -> float:
    return (n / d * 100.0) if d > 0 else 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--sampled", required=True)
    ap.add_argument("--run-id", default=None)
    ap.add_argument(
        "--endpoint-keys",
        default="http.route,http.target",
        help="Comma-separated endpoint attribute keys (priority order)",
    )
    ap.add_argument(
        "--critical-values",
        default=",".join(DEFAULT_CRITICAL),
        help="Comma-separated critical endpoint values",
    )
    ap.add_argument("--top", type=int, default=20, help="Top endpoints to print")
    args = ap.parse_args()

    baseline_path = Path(args.baseline)
    sampled_path = Path(args.sampled)
    endpoint_keys = [x.strip() for x in args.endpoint_keys.split(",") if x.strip()]
    critical_values = {x.strip() for x in args.critical_values.split(",") if x.strip()}

    base = _extract_trace_info(baseline_path, args.run_id, endpoint_keys)
    samp = _extract_trace_info(sampled_path, args.run_id, endpoint_keys)

    base_ids = set(base.keys())
    samp_ids = set(samp.keys())
    kept_ids = base_ids & samp_ids

    base_with_ep = {tid for tid, t in base.items() if t.endpoint is not None}
    base_without_ep = base_ids - base_with_ep

    base_critical = {tid for tid, t in base.items() if t.endpoint in critical_values}
    base_non_critical = {tid for tid, t in base.items() if t.endpoint is not None and t.endpoint not in critical_values}

    kept_critical = kept_ids & base_critical
    kept_non_critical = kept_ids & base_non_critical

    print("== INPUT ==")
    print(f"baseline: {baseline_path}")
    print(f"sampled : {sampled_path}")
    print(f"run_id  : {args.run_id or '(none)'}")
    print(f"endpoint keys: {endpoint_keys}")
    print(f"critical values ({len(critical_values)}): {sorted(critical_values)}")
    print()

    print("== COVERAGE ==")
    print(f"baseline traces total            : {len(base_ids)}")
    print(f"baseline traces with endpoint    : {len(base_with_ep)} ({_pct(len(base_with_ep), len(base_ids)):.2f}%)")
    print(f"baseline traces without endpoint : {len(base_without_ep)} ({_pct(len(base_without_ep), len(base_ids)):.2f}%)")
    if len(base_with_ep) == 0:
        print("NOTE: endpoint coverage is 0%. Cannot prove endpoint-priority behavior with this dataset.")
    print()

    print("== RETENTION BY GROUP ==")
    print(f"kept traces overall              : {len(kept_ids)}/{len(base_ids)} ({_pct(len(kept_ids), len(base_ids)):.2f}%)")
    print(
        f"critical endpoint kept           : {len(kept_critical)}/{len(base_critical)} "
        f"({_pct(len(kept_critical), len(base_critical)):.2f}%)"
    )
    print(
        f"non-critical endpoint kept       : {len(kept_non_critical)}/{len(base_non_critical)} "
        f"({_pct(len(kept_non_critical), len(base_non_critical)):.2f}%)"
    )
    if len(base_critical) > 0 and len(base_non_critical) > 0:
        delta = _pct(len(kept_critical), len(base_critical)) - _pct(len(kept_non_critical), len(base_non_critical))
        print(f"critical uplift vs non-critical  : {delta:+.2f} điểm %")
    print()

    by_endpoint_total: Dict[str, int] = defaultdict(int)
    by_endpoint_kept: Dict[str, int] = defaultdict(int)
    for tid, info in base.items():
        if not info.endpoint:
            continue
        by_endpoint_total[info.endpoint] += 1
        if tid in kept_ids:
            by_endpoint_kept[info.endpoint] += 1

    print("== PER-ENDPOINT (TOP BY BASELINE COUNT) ==")
    ranked = sorted(by_endpoint_total.items(), key=lambda kv: kv[1], reverse=True)[: max(1, args.top)]
    if not ranked:
        print("(no endpoint-labeled traces)")
        return 0

    print(f"{'endpoint':40s} {'kept':>12s} {'rate':>10s} {'critical':>10s}")
    for endpoint, total in ranked:
        kept = by_endpoint_kept.get(endpoint, 0)
        rate = _pct(kept, total)
        tag = "yes" if endpoint in critical_values else "no"
        print(f"{endpoint[:40]:40s} {f'{kept}/{total}':>12s} {f'{rate:.2f}%':>10s} {tag:>10s}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
