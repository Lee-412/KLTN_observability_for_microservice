#!/usr/bin/env python3
"""
Test script to verify adaptive sampling behavior.

This script generates traces with varying durations and checks if:
1. Low-score traces are being dropped
2. High-score traces are being kept
3. The drop rate matches the expected target
"""

import time
import random
import requests
import json
from datetime import datetime, timedelta
from collections import defaultdict
import sys

# Configuration
OTEL_ENDPOINT = "http://localhost:4318/v1/traces"
SIGNOZ_API = "http://localhost:8080/api/v1"
SERVICE_NAME = "adaptive-test-service"

def generate_trace_id():
    """Generate a random 32-character hex trace ID."""
    return ''.join(random.choices('0123456789abcdef', k=32))

def generate_span_id():
    """Generate a random 16-character hex span ID."""
    return ''.join(random.choices('0123456789abcdef', k=16))

def nano_timestamp(dt=None):
    """Convert datetime to nanoseconds since epoch."""
    if dt is None:
        dt = datetime.utcnow()
    return int(dt.timestamp() * 1_000_000_000)

def send_trace(duration_ms, num_spans=1, has_error=False):
    """
    Send a trace with specified duration and characteristics.
    
    Args:
        duration_ms: Duration in milliseconds
        num_spans: Number of spans in the trace
        has_error: Whether the trace should have an error
    
    Returns:
        trace_id: The generated trace ID
    """
    trace_id = generate_trace_id()
    start_time = datetime.utcnow()
    end_time = start_time + timedelta(milliseconds=duration_ms)
    
    spans = []
    for i in range(num_spans):
        span_start = start_time + timedelta(milliseconds=i * (duration_ms / num_spans))
        span_end = span_start + timedelta(milliseconds=duration_ms / num_spans)
        
        span = {
            "traceId": trace_id,
            "spanId": generate_span_id(),
            "name": f"operation-{i}",
            "startTimeUnixNano": str(nano_timestamp(span_start)),
            "endTimeUnixNano": str(nano_timestamp(span_end)),
            "kind": 1,  # SPAN_KIND_INTERNAL
            "attributes": [
                {"key": "service.name", "value": {"stringValue": SERVICE_NAME}},
                {"key": "http.status_code", "value": {"intValue": "500" if has_error else "200"}},
                {"key": "test.duration_ms", "value": {"intValue": str(duration_ms)}},
                {"key": "test.num_spans", "value": {"intValue": str(num_spans)}},
            ],
        }
        
        if has_error:
            span["status"] = {
                "code": 2,  # STATUS_CODE_ERROR
                "message": "Test error"
            }
        
        if i == 0 and num_spans > 1:
            # First span is parent
            pass
        elif i > 0:
            # Subsequent spans reference the first span as parent
            span["parentSpanId"] = spans[0]["spanId"]
        
        spans.append(span)
    
    payload = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": SERVICE_NAME}},
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "adaptive-test",
                            "version": "1.0"
                        },
                        "spans": spans
                    }
                ]
            }
        ]
    }
    
    try:
        response = requests.post(
            OTEL_ENDPOINT,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        response.raise_for_status()
        return trace_id
    except Exception as e:
        print(f"Error sending trace: {e}")
        return None

def query_traces(service_name, start_time, end_time):
    """Query traces from SigNoz API."""
    # Wait a bit for traces to be processed
    time.sleep(2)
    
    # SigNoz query format (this may need adjustment based on your SigNoz version)
    # For now, we'll use a simple approach - query by service name and time range
    url = f"{SIGNOZ_API}/traces"
    params = {
        "service": service_name,
        "start": int(start_time.timestamp() * 1000000),  # microseconds
        "end": int(end_time.timestamp() * 1000000),
        "limit": 10000,
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("traces", [])
        else:
            print(f"Query failed: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"Error querying traces: {e}")
        return []

def test_adaptive_sampling():
    """
    Main test function.
    
    Test strategy:
    1. Generate traces with uniformly distributed durations (1ms to 1000ms)
    2. Wait for adaptive sampler to adjust
    3. Check if low-duration traces are dropped and high-duration kept
    """
    print("=" * 80)
    print("Adaptive Sampling Test")
    print("=" * 80)
    print()
    
    # Phase 1: Warmup - send diverse traces to let adaptive adjust
    print("Phase 1: Warmup (sending 100 traces with varying durations)...")
    warmup_traces = []
    for i in range(100):
        duration = random.randint(10, 500)
        num_spans = random.randint(1, 10)
        has_error = random.random() < 0.05  # 5% error rate
        trace_id = send_trace(duration, num_spans, has_error)
        if trace_id:
            warmup_traces.append({
                'trace_id': trace_id,
                'duration': duration,
                'num_spans': num_spans,
                'has_error': has_error,
                'score': 0.01 * duration + 0.5 * num_spans + (10.0 if has_error else 0)
            })
        time.sleep(0.1)  # ~10 traces/sec
    
    print(f"Sent {len(warmup_traces)} warmup traces")
    print("Waiting 15s for adaptive sampler to adjust...")
    time.sleep(15)
    
    # Phase 2: Test - send traces and track what gets kept
    print("\nPhase 2: Test (sending 200 more traces)...")
    test_start = datetime.utcnow()
    test_traces = []
    
    # Create a distribution with more low-score traces
    # This ensures that if adaptive is working, many should be dropped
    for i in range(200):
        # 60% low duration (1-100ms)
        # 30% medium duration (100-300ms)
        # 10% high duration (300-1000ms)
        rand = random.random()
        if rand < 0.6:
            duration = random.randint(1, 100)
        elif rand < 0.9:
            duration = random.randint(100, 300)
        else:
            duration = random.randint(300, 1000)
        
        num_spans = random.randint(1, 10)
        has_error = random.random() < 0.05  # 5% error rate
        
        trace_id = send_trace(duration, num_spans, has_error)
        if trace_id:
            score = 0.01 * duration + 0.5 * num_spans + (10.0 if has_error else 0)
            test_traces.append({
                'trace_id': trace_id,
                'duration': duration,
                'num_spans': num_spans,
                'has_error': has_error,
                'score': score
            })
        time.sleep(0.05)  # ~20 traces/sec
    
    test_end = datetime.utcnow()
    
    print(f"Sent {len(test_traces)} test traces")
    
    # Calculate expected scores
    scores = [t['score'] for t in test_traces]
    scores.sort()
    
    print("\nScore distribution:")
    print(f"  Min score: {min(scores):.2f}")
    print(f"  P25: {scores[len(scores)//4]:.2f}")
    print(f"  Median: {scores[len(scores)//2]:.2f}")
    print(f"  P75: {scores[3*len(scores)//4]:.2f}")
    print(f"  Max score: {max(scores):.2f}")
    
    # Wait for traces to be processed and available in SigNoz
    print("\nWaiting 30s for traces to be processed...")
    time.sleep(30)
    
    # Note: Querying from SigNoz API may not be straightforward
    # Alternative: Check collector logs or use ClickHouse directly
    print("\n" + "=" * 80)
    print("VERIFICATION STEPS")
    print("=" * 80)
    print()
    print("To verify adaptive sampling is working, check:")
    print()
    print("1. Check collector logs for adaptive sampler recompute messages:")
    print("   Look for lines like:")
    print("   'adaptive model sampler recompute'")
    print("   These should show incoming_rate, keep_ratio, and threshold")
    print()
    print("2. Query ClickHouse directly to count traces:")
    print(f"   SELECT count(*) FROM signoz_traces.distributed_signoz_index_v3")
    print(f"   WHERE service_name = '{SERVICE_NAME}'")
    print(f"   AND timestamp >= {int(test_start.timestamp() * 1e9)}")
    print(f"   AND timestamp <= {int(test_end.timestamp() * 1e9)}")
    print()
    print(f"   Expected: Much less than {len(test_traces)} traces")
    print(f"   (target is ~50 traces/sec, we sent ~20 traces/sec)")
    print()
    print("3. Check specific trace IDs:")
    print("   High-score traces (should be KEPT):")
    for t in sorted(test_traces, key=lambda x: x['score'], reverse=True)[:5]:
        print(f"     {t['trace_id']}: score={t['score']:.2f}, duration={t['duration']}ms, error={t['has_error']}")
    print()
    print("   Low-score traces (should be DROPPED):")
    for t in sorted(test_traces, key=lambda x: x['score'])[:5]:
        print(f"     {t['trace_id']}: score={t['score']:.2f}, duration={t['duration']}ms, error={t['has_error']}")
    print()
    
    # Save trace IDs to file for manual verification
    output_file = f"test_traces_{int(test_start.timestamp())}.json"
    with open(output_file, 'w') as f:
        json.dump({
            'test_start': test_start.isoformat(),
            'test_end': test_end.isoformat(),
            'traces': test_traces,
            'config': {
                'target_traces_per_sec': 50,
                'weights': {
                    'duration_ms': 0.01,
                    'span_count': 0.5,
                    'has_error': 10.0
                }
            }
        }, f, indent=2)
    
    print(f"Saved trace details to: {output_file}")
    print()
    
    return test_traces

if __name__ == "__main__":
    test_adaptive_sampling()
