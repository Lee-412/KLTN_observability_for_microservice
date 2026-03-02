package sampling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func TestAdaptiveLinearModelSampler_TargetTracesPerSec_DropsLowScores(t *testing.T) {
	weights := map[string]float64{
		"duration_ms": 1.0,
		"has_error":   0.0,
		"span_count":  0.0,
	}

	now := time.Unix(0, 0)
	step := 10 * time.Millisecond // ~100 traces/sec

	sampler := NewAdaptiveLinearModelSampler(zap.NewNop(), AdaptiveLinearModelSamplerConfig{
		FallbackThreshold:  0,
		Intercept:          0,
		Weights:            weights,
		WindowDuration:     10 * time.Second,
		RecomputeInterval:  time.Nanosecond,
		MaxSamples:         2048,
		TargetTracesPerSec: 10,
		AlwaysKeepErrors:   true,
		TimeNow: func() time.Time {
			return now
		},
	})

	traceID := pcommon.TraceID([16]byte{1})
	baseStart := time.Unix(1, 0)

	mkTrace := func(d time.Duration) *TraceData {
		return newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
			{StartTime: baseStart, Duration: d, Status: ptrace.StatusCodeOk, Attrs: map[string]any{"http.status_code": int64(200)}},
		})
	}

	// Warm-up: feed a monotonic score distribution.
	for i := 1; i <= 100; i++ {
		now = now.Add(step)
		_, _ = sampler.Evaluate(traceID, mkTrace(time.Duration(i)*time.Millisecond))
	}

	// Measure: second pass should keep roughly top ~10% (target 10 kept per 100/sec).
	sampled := 0
	for i := 1; i <= 100; i++ {
		now = now.Add(step)
		dec, err := sampler.Evaluate(traceID, mkTrace(time.Duration(i)*time.Millisecond))
		assert.NoError(t, err)
		if dec == Sampled {
			sampled++
		}
	}

	// Quantile ties/rounding make this approximate.
	assert.GreaterOrEqual(t, sampled, 5)
	assert.LessOrEqual(t, sampled, 25)
}

func TestAdaptiveLinearModelSampler_AlwaysKeepErrors(t *testing.T) {
	weights := map[string]float64{
		"duration_ms": 1.0,
		"has_error":   0.0,
		"span_count":  0.0,
	}

	now := time.Unix(0, 0)
	sampler := NewAdaptiveLinearModelSampler(zap.NewNop(), AdaptiveLinearModelSamplerConfig{
		FallbackThreshold: 0,
		Intercept:         0,
		Weights:           weights,
		WindowDuration:    10 * time.Second,
		RecomputeInterval: time.Nanosecond,
		MaxSamples:        512,
		KeepRatio:         0.01,
		AlwaysKeepErrors:  true,
		TimeNow: func() time.Time {
			return now
		},
	})

	traceID := pcommon.TraceID([16]byte{1})

	// Warm-up with high scores so threshold becomes high.
	for i := 0; i < 200; i++ {
		now = now.Add(10 * time.Millisecond)
		_, _ = sampler.Evaluate(traceID, newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
			{StartTime: time.Unix(1, 0), Duration: 500 * time.Millisecond, Status: ptrace.StatusCodeOk, Attrs: map[string]any{"http.status_code": int64(200)}},
		}))
	}

	// A very low-score error trace should still be kept.
	now = now.Add(10 * time.Millisecond)
	errTrace := newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
		{StartTime: time.Unix(1, 0), Duration: 1 * time.Millisecond, Status: ptrace.StatusCodeError, Attrs: map[string]any{"http.status_code": int64(500)}},
	})
	dec, err := sampler.Evaluate(traceID, errTrace)
	assert.NoError(t, err)
	assert.Equal(t, Sampled, dec)
}
