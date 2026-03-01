#!/usr/bin/env python3
"""Evaluate A/B tail-sampling results from two fileexporter OTLP JSON outputs.

Assumes each line is an OTLP JSON record (exporter 'file' format=json).

Outputs:
- baseline traces count
- sampled traces count
- kept/dropped trace rate (by trace_id)
- error trace retention
- breakdown by `test.case` attribute when present

Usage:
  /home/leeduc/miniconda3/bin/python scripts/evaluate_ab_sampling.py \
    --baseline signoz/deploy/docker/otel-export/latencytest_baseline/traces.baseline.json \
    --sampled  signoz/deploy/docker/otel-export/latencytest_sampled/traces.sampled.json \
    --run-id 20260225-040156
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class SpanRow:
    trace_id: str
    has_error: bool
    attrs: Dict[str, Any]


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
        # already hex
        return any_val.lower()
    if isinstance(any_val, list):
        # array of ints
        try:
            return bytes(int(x) & 0xFF for x in any_val).hex()
        except Exception:
            return None
    return None


def _attrs_from(span: dict) -> Dict[str, Any]:
    """Extract span attributes into a simple dict.

    Supports multiple OTLP JSON shapes seen in fileexporter outputs:
    - attributes: [{"key":"k","value":{...}}, ...]
    - attributes: {"keyValues": [{"key":"k","value":{...}}, ...]}
    """

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
        k = kv.get("key")
        v = kv.get("value")
        if not isinstance(k, str) or not isinstance(v, dict):
            continue

        # The value is oneof: stringValue/intValue/doubleValue/boolValue/bytesValue
        for field in (
            "stringValue",
            "intValue",
            "doubleValue",
            "boolValue",
            "bytesValue",
        ):
            if field in v:
                out[k] = v[field]
                break

    return out


def _span_has_error(span: dict, attrs: Dict[str, Any]) -> bool:
    status = span.get("status")
    if isinstance(status, dict):
        code = status.get("code")
        # In OTLP JSON, code may be 2 for ERROR.
        if code == 2 or code == "STATUS_CODE_ERROR":
            return True

    http_code = attrs.get("http.status_code")
    try:
        if http_code is not None and int(http_code) >= 500:
            return True
    except Exception:
        pass

    return False


def _extract_spans(path: Path) -> List[SpanRow]:
    rows: List[SpanRow] = []
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
                    rows.append(SpanRow(trace_id=tid, has_error=_span_has_error(sp, attrs), attrs=attrs))
    return rows


def _trace_level(rows: List[SpanRow], run_id: Optional[str]) -> Dict[str, dict]:
    # trace_id -> summary
    out: Dict[str, dict] = {}
    for r in rows:
        if run_id is not None and r.attrs.get("test.run_id") != run_id:
            continue
        cur = out.get(r.trace_id)
        if cur is None:
            cur = {
                "trace_id": r.trace_id,
                "has_error": False,
                "case": r.attrs.get("test.case"),
            }
            out[r.trace_id] = cur
        if r.has_error:
            cur["has_error"] = True
        # keep first non-null case
        if cur.get("case") is None and r.attrs.get("test.case") is not None:
            cur["case"] = r.attrs.get("test.case")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--sampled", required=True)
    ap.add_argument("--run-id", default=None, help="Optional filter by test.run_id")
    args = ap.parse_args()

    baseline_path = Path(args.baseline)
    sampled_path = Path(args.sampled)

    baseline_rows = _extract_spans(baseline_path)
    sampled_rows = _extract_spans(sampled_path)

    baseline_traces = _trace_level(baseline_rows, args.run_id)
    sampled_traces = _trace_level(sampled_rows, args.run_id)

    base_ids: Set[str] = set(baseline_traces.keys())
    samp_ids: Set[str] = set(sampled_traces.keys())

    kept_ids = base_ids & samp_ids
    dropped_ids = base_ids - samp_ids

    print("== INPUT ==")
    print(f"baseline: {args.baseline}")
    print(f"sampled : {args.sampled}")
    print(f"run_id  : {args.run_id or '(none)'}")
    print()

    print("== COUNTS ==")
    print(f"baseline traces: {len(base_ids)}")
    print(f"sampled  traces: {len(samp_ids)}")
    if len(base_ids) > 0:
        print(f"kept traces    : {len(kept_ids)}/{len(base_ids)} ({len(kept_ids)/len(base_ids)*100:.2f}%)")
        print(f"dropped traces : {len(dropped_ids)}/{len(base_ids)} ({len(dropped_ids)/len(base_ids)*100:.2f}%)")
    print()

    print("== ERROR RETENTION ==")
    base_err = [t for t in baseline_traces.values() if t["has_error"]]
    samp_err = [t for t in sampled_traces.values() if t["has_error"]]
    kept_err = [t for t in base_err if t["trace_id"] in kept_ids]
    print(f"baseline error traces: {len(base_err)}/{len(base_ids)} ({(len(base_err)/len(base_ids)*100 if base_ids else 0):.2f}%)")
    print(f"sampled  error traces: {len(samp_err)}/{len(samp_ids)} ({(len(samp_err)/len(samp_ids)*100 if samp_ids else 0):.2f}%)")
    if len(base_err) > 0:
        print(f"kept error traces    : {len(kept_err)}/{len(base_err)} ({len(kept_err)/len(base_err)*100:.2f}%)")
    print()

    print("== BY test.case ==")
    base_by_case: Dict[str, List[str]] = defaultdict(list)
    for tid, t in baseline_traces.items():
        case = t.get("case") or "(none)"
        base_by_case[str(case)].append(tid)

    for case, tids in sorted(base_by_case.items()):
        tids_set = set(tids)
        kept = tids_set & kept_ids
        errs = [tid for tid in tids if baseline_traces[tid]["has_error"]]
        kept_errs = [tid for tid in errs if tid in kept_ids]
        print(f"case={case}")
        print(f"  baseline traces: {len(tids)}")
        if len(tids) > 0:
            print(f"  kept traces    : {len(kept)}/{len(tids)} ({len(kept)/len(tids)*100:.2f}%)")
        if len(errs) > 0:
            print(f"  error traces   : {len(errs)}/{len(tids)} ({len(errs)/len(tids)*100:.2f}%)")
            print(f"  kept error     : {len(kept_errs)}/{len(errs)} ({len(kept_errs)/len(errs)*100:.2f}%)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
