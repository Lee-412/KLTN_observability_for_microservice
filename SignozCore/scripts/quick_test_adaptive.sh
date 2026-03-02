#!/bin/bash
# Quick script to apply optimized adaptive sampling config and test

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DEPLOY_DIR="$SCRIPT_DIR/../signoz/deploy/docker"

echo "=========================================="
echo "Adaptive Sampling - Quick Test"
echo "=========================================="
echo ""

# Step 1: Backup current config
echo "1. Backing up current config..."
if [ -f "$DEPLOY_DIR/otel-collector-config.yaml" ]; then
    cp "$DEPLOY_DIR/otel-collector-config.yaml" "$DEPLOY_DIR/otel-collector-config.yaml.backup.$(date +%s)"
    echo "   ✓ Backed up to otel-collector-config.yaml.backup.*"
fi

# Step 2: Apply optimized config
echo ""
echo "2. Applying Hotrod-optimized config..."
cp "$DEPLOY_DIR/otel-collector-config-adaptive.yaml" "$DEPLOY_DIR/otel-collector-config.yaml"
echo "   ✓ Config updated"

# Step 3: Restart collector
echo ""
echo "3. Restarting otel-collector..."
cd "$DEPLOY_DIR"

if command -v docker-compose &> /dev/null; then
    docker-compose restart signoz-otel-collector
elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
    docker compose restart signoz-otel-collector
else
    echo "   ⚠️  Could not find docker-compose or docker compose"
    echo "   Please restart manually: docker restart signoz-otel-collector"
    exit 1
fi

echo "   ✓ Collector restarted"

# Step 4: Wait for collector to be ready
echo ""
echo "4. Waiting for collector to be ready..."
sleep 10
echo "   ✓ Ready"

# Step 5: Check logs
echo ""
echo "5. Checking logs for adaptive sampler initialization..."
echo ""

CONTAINER="signoz-otel-collector"

if [ -z "$CONTAINER" ]; then
    echo "   ⚠️  Could not find otel-collector container"
else
    echo "Recent logs:"
    echo "----------------------------------------"
    docker logs --tail 50 "$CONTAINER" 2>&1 | grep -i "adaptive" || echo "   (No adaptive logs yet - may need more time)"
    echo "----------------------------------------"
fi

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Expected score distribution with new config:"
echo "  - Fast requests (100ms, 5 spans):   score = 15"
echo "  - Typical requests (200ms, 8 spans): score = 28"
echo "  - Slow requests (500ms, 12 spans):   score = 62"
echo "  - Errors (any duration):             score = 100+"
echo ""
echo "With threshold ~40-50:"
echo "  → Fast requests DROPPED ⬇"
echo "  → Typical requests DROPPED ⬇"
echo "  → Slow requests KEPT ✓"
echo "  → All errors KEPT ✓"
echo ""
echo "Next steps:"
echo ""
echo "  1. Start Hotrod (if not running):"
echo "     docker run -p 8080:8080 jaegertracing/example-hotrod:latest all"
echo ""
echo "  2. Generate some traffic:"
echo "     # Open http://localhost:8080 and click the buttons"
echo ""
echo "  3. Monitor adaptive sampling in another terminal:"
echo "     cd $SCRIPT_DIR"
echo "     ./monitor_adaptive_logs.sh"
echo ""
echo "  4. After 30-60 seconds, check if traces are being dropped:"
echo "     docker exec -it <clickhouse-container> clickhouse-client"
echo "     SELECT count(*) FROM signoz_traces.distributed_signoz_index_v3"
echo "     WHERE timestamp > now() - INTERVAL 5 MINUTE"
echo ""
echo "  5. Or run the test script:"
echo "     cd $SCRIPT_DIR"
echo "     python test_adaptive_sampling.py"
echo ""
echo "Look for these in logs (monitor_adaptive_logs.sh):"
echo "  ✓ 'adaptive model sampler initialized'"
echo "  ✓ 'adaptive model sampler recompute' (every 10s)"
echo "  ✓ 'keep_ratio' < 1.0 (indicating drops)"
echo "  ✓ 'threshold' changing based on traffic"
echo ""

# Offer to start monitoring
echo "Start monitoring logs now? (y/n)"
read -r response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    exec "$SCRIPT_DIR/monitor_adaptive_logs.sh"
fi
