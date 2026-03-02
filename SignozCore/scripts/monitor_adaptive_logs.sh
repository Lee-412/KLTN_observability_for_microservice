#!/bin/bash
# Script to monitor otel-collector logs for adaptive sampling activity
















































echo "  ./monitor_adaptive_logs.sh"echo "  cd $(dirname "$0")"echo "Or use the monitoring script:"echo ""echo "  docker logs -f signoz-otel-collector 2>&1 | grep -i adaptive"echo "Monitor logs with:"echo ""echo "✓ Done!"echo ""echo "=========================================="docker logs --tail 30 signoz-otel-collector 2>&1 | grep -i "adaptive\|tail_sampling" || echo "(No sampling logs yet - wait a bit)"echo "=========================================="echo "Recent logs:"echo ""# Check logssleep 10echo "Waiting 10s for collector to start..."# Waitecho ""echo "✓ Restarted"docker restart signoz-otel-collectorecho "Restarting collector..."# Restartecho ""echo "✓ Applied adaptive config"cp otel-collector-config-adaptive.yaml otel-collector-config.yaml# Applyfi    echo "✓ Backed up current config"    cp otel-collector-config.yaml "otel-collector-config.yaml.backup.$(date +%s)"if [ -f "otel-collector-config.yaml" ]; then# Backupecho ""echo "=========================================="echo "Apply Adaptive Sampling Config"echo "=========================================="cd "$(dirname "$0")/../signoz/deploy/docker"set -e
echo "=========================================="
echo "Monitoring Adaptive Sampling Logs"
echo "=========================================="
echo ""

# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: docker command not found"
    exit 1
fi

# Find the otel-collector container
CONTAINER="signoz-otel-collector"

if ! docker ps --format "{{.Names}}" | grep -q "^${CONTAINER}$"; then
    echo "Error: Container ${CONTAINER} not found or not running"
    echo "Make sure the collector is running with: docker ps"
    exit 1
fi

echo "Monitoring container: $CONTAINER"
echo ""
echo "Looking for adaptive sampler log messages..."
echo "Press Ctrl+C to stop"
echo ""
echo "=========================================="
echo ""

# Follow logs and filter for adaptive sampling messages
docker logs -f "$CONTAINER" 2>&1 | grep --line-buffered -E "adaptive|recompute|threshold|keep_ratio|incoming_rate" | while read -r line; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $line"
done
