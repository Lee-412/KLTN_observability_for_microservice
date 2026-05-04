#!/bin/bash
# Simple script to apply adaptive sampling config for docker deployment

set -e

cd "$(dirname "$0")/../signoz/deploy/docker"

echo "=========================================="
echo "Apply Adaptive Sampling Config"
echo "=========================================="
echo ""

# Backup
if [ -f "otel-collector-config.yaml" ]; then
    cp otel-collector-config.yaml "otel-collector-config.yaml.backup.$(date +%s)"
    echo "✓ Backed up current config"
fi

# Apply
cp otel-collector-config-adaptive.yaml otel-collector-config.yaml
echo "✓ Applied adaptive config"
echo ""

# Restart
echo "Restarting collector..."
docker restart signoz-otel-collector
echo "✓ Restarted"
echo ""

# Wait
echo "Waiting 10s for collector to start..."
sleep 10

# Check logs
echo ""
echo "Recent logs:"
echo "=========================================="
docker logs --tail 30 signoz-otel-collector 2>&1 | grep -i "adaptive\|tail_sampling" || echo "(No sampling logs yet - wait a bit)"
echo "=========================================="
echo ""

echo "✓ Done!"
echo ""
echo "Monitor logs with:"
echo "  docker logs -f signoz-otel-collector 2>&1 | grep -i adaptive"
echo ""
echo "Or use the monitoring script:"
echo "  cd $(dirname "$0")"
echo "  ./monitor_adaptive_logs.sh"
