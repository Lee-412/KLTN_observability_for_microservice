#!/usr/bin/env python3
"""
Verify adaptive sampling results by querying ClickHouse directly.

This script checks:
1. How many traces were actually stored vs sent
2. Score distribution of kept vs dropped traces
3. Whether high-score traces are kept and low-score dropped
"""

import json
import sys
from datetime import datetime
import clickhouse_connect

# Configuration
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
SERVICE_NAME = "adaptive-test-service"

def load_test_data(json_file):
    """Load test trace data from JSON file."""
    try:
        with open(json_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading test data: {e}")
        return None

def query_clickhouse(query):
    """Execute a query against ClickHouse."""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
        )
        result = client.query(query)
        return result.result_rows
    except Exception as e:
        print(f"Error querying ClickHouse: {e}")
        return None

def verify_sampling(test_data_file):
    """Verify adaptive sampling results."""
    print("=" * 80)
    print("Adaptive Sampling Verification")
    print("=" * 80)
    print()
    
    # Load test data
    test_data = load_test_data(test_data_file)
    if not test_data:
        print(f"Failed to load test data from {test_data_file}")
        return
    
    traces = test_data['traces']
    test_start = datetime.fromisoformat(test_data['test_start'])
    test_end = datetime.fromisoformat(test_data['test_end'])
    
    print(f"Test period: {test_start} to {test_end}")
    print(f"Total traces sent: {len(traces)}")
    print()
    
    # Query total traces stored
    start_ns = int(test_start.timestamp() * 1e9)
    end_ns = int(test_end.timestamp() * 1e9)
    
    query = f"""
    SELECT count(DISTINCT trace_id) as trace_count
    FROM signoz_traces.distributed_signoz_index_v3
    WHERE serviceName = '{SERVICE_NAME}'
    AND timestamp >= {start_ns}
    AND timestamp <= {end_ns}
    """
    
    print("Querying ClickHouse...")
    result = query_clickhouse(query)
    
    if result:
        stored_count = result[0][0]
        drop_rate = (len(traces) - stored_count) / len(traces) * 100
        
        print(f"Traces stored in ClickHouse: {stored_count}")
        print(f"Traces dropped: {len(traces) - stored_count}")
        print(f"Drop rate: {drop_rate:.1f}%")
        print()
        
        if stored_count == len(traces):
            print("⚠️  WARNING: No traces were dropped!")
            print("   This indicates adaptive sampling may not be working correctly.")
            print()
        elif stored_count < len(traces) * 0.3:
            print("✓ Adaptive sampling appears to be working!")
            print("  Significant number of traces were dropped.")
            print()
        else:
            print("⚠️  Partial drop detected, but rate may be lower than expected.")
            print()
    
    # Check specific trace IDs
    print("Checking specific trace IDs...")
    print()
    
    # Sort traces by score
    traces_by_score = sorted(traces, key=lambda x: x['score'])
    
    # Check bottom 10 (lowest scores - should be dropped)
    low_score_traces = traces_by_score[:10]
    print("Low-score traces (should be DROPPED):")
    for t in low_score_traces:
        query = f"""
        SELECT count(*)
        FROM signoz_traces.distributed_signoz_index_v3
        WHERE traceID = '{t['trace_id']}'
        """
        result = query_clickhouse(query)
        kept = result[0][0] > 0 if result else False
        status = "KEPT ❌" if kept else "DROPPED ✓"
        print(f"  {t['trace_id']}: score={t['score']:.2f}, duration={t['duration']}ms - {status}")
    
    print()
    
    # Check top 10 (highest scores - should be kept)
    high_score_traces = traces_by_score[-10:]
    print("High-score traces (should be KEPT):")
    for t in high_score_traces:
        query = f"""
        SELECT count(*)
        FROM signoz_traces.distributed_signoz_index_v3
        WHERE traceID = '{t['trace_id']}'
        """
        result = query_clickhouse(query)
        kept = result[0][0] > 0 if result else False
        status = "KEPT ✓" if kept else "DROPPED ❌"
        print(f"  {t['trace_id']}: score={t['score']:.2f}, duration={t['duration']}ms - {status}")
    
    print()
    
    # Check error traces (should always be kept if always_keep_errors=true)
    error_traces = [t for t in traces if t['has_error']]
    if error_traces:
        print(f"Error traces ({len(error_traces)} total, should ALL be KEPT):")
        for t in error_traces[:5]:  # Show first 5
            query = f"""
            SELECT count(*)
            FROM signoz_traces.distributed_signoz_index_v3
            WHERE traceID = '{t['trace_id']}'
            """
            result = query_clickhouse(query)
            kept = result[0][0] > 0 if result else False
            status = "KEPT ✓" if kept else "DROPPED ❌"
            print(f"  {t['trace_id']}: score={t['score']:.2f}, duration={t['duration']}ms - {status}")
        print()
    
    # Statistical analysis
    print("=" * 80)
    print("STATISTICAL ANALYSIS")
    print("=" * 80)
    print()
    
    # Get all kept trace IDs
    query = f"""
    SELECT DISTINCT traceID
    FROM signoz_traces.distributed_signoz_index_v3
    WHERE serviceName = '{SERVICE_NAME}'
    AND timestamp >= {start_ns}
    AND timestamp <= {end_ns}
    """
    result = query_clickhouse(query)
    
    if result:
        kept_trace_ids = {row[0] for row in result}
        
        # Separate kept and dropped
        kept_traces = [t for t in traces if t['trace_id'] in kept_trace_ids]
        dropped_traces = [t for t in traces if t['trace_id'] not in kept_trace_ids]
        
        if kept_traces:
            kept_scores = [t['score'] for t in kept_traces]
            kept_durations = [t['duration'] for t in kept_traces]
            
            print("KEPT traces statistics:")
            print(f"  Count: {len(kept_traces)}")
            print(f"  Score - min: {min(kept_scores):.2f}, median: {sorted(kept_scores)[len(kept_scores)//2]:.2f}, max: {max(kept_scores):.2f}")
            print(f"  Duration - min: {min(kept_durations)}ms, median: {sorted(kept_durations)[len(kept_durations)//2]}ms, max: {max(kept_durations)}ms")
            print()
        
        if dropped_traces:
            dropped_scores = [t['score'] for t in dropped_traces]
            dropped_durations = [t['duration'] for t in dropped_traces]
            
            print("DROPPED traces statistics:")
            print(f"  Count: {len(dropped_traces)}")
            print(f"  Score - min: {min(dropped_scores):.2f}, median: {sorted(dropped_scores)[len(dropped_scores)//2]:.2f}, max: {max(dropped_scores):.2f}")
            print(f"  Duration - min: {min(dropped_durations)}ms, median: {sorted(dropped_durations)[len(dropped_durations)//2]}ms, max: {max(dropped_durations)}ms")
            print()
        
        if kept_traces and dropped_traces:
            threshold_estimate = min(kept_scores)
            print(f"Estimated threshold: {threshold_estimate:.2f}")
            print(f"(This is the minimum score among kept traces)")
            print()
            
            # Check if separation is clear
            if min(kept_scores) > max(dropped_scores):
                print("✓ Perfect separation: all kept traces have higher scores than all dropped traces")
            else:
                overlap = sum(1 for t in dropped_traces if t['score'] >= min(kept_scores))
                print(f"⚠️  Some overlap: {overlap} dropped traces have scores >= minimum kept score")
                print("   This is expected due to probabilistic tie-breaking at the threshold")

def main():
    if len(sys.argv) < 2:
        print("Usage: python verify_adaptive_sampling.py <test_traces_json_file>")
        print()
        print("Example:")
        print("  python verify_adaptive_sampling.py test_traces_1234567890.json")
        sys.exit(1)
    
    test_data_file = sys.argv[1]
    verify_sampling(test_data_file)

if __name__ == "__main__":
    main()
