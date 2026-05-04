package sampling

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func TestLinearModelSampler_Evaluate(t *testing.T) {
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})

	weights := map[string]float64{
		"duration_ms": 0.01,
		"has_error":   2.0,
		"span_count":  0.1,
	}

	// sampled if score >= 2.0
	// score = 0.01*duration_ms + 2*has_error + 0.1*span_count
	sampler := NewLinearModelSampler(zap.NewNop(), 2.0, 0.0, weights)

	now := time.Now()

	cases := []struct {
		name  string
		trace *TraceData
		want  Decision
	}{
		{
			name: "short trace without error -> not sampled",
			trace: newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
				{StartTime: now, Duration: 50 * time.Millisecond, Status: ptrace.StatusCodeOk, Attrs: map[string]any{"http.status_code": int64(200)}},
			}),
			want: NotSampled,
		},
		{
			name: "longer trace without error but enough score -> sampled",
			trace: newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
				{StartTime: now, Duration: 400 * time.Millisecond, Status: ptrace.StatusCodeOk, Attrs: map[string]any{"http.status_code": int64(200)}},
			}),
			want: Sampled,
		},
		{
			name: "error trace -> sampled",
			trace: newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
				{StartTime: now, Duration: 10 * time.Millisecond, Status: ptrace.StatusCodeError, Attrs: map[string]any{"http.status_code": int64(500)}},
			}),
			want: Sampled,
		},
		{
			name: "status OK but http.status_code=500 -> sampled (has_error feature)",
			trace: newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
				{StartTime: now, Duration: 10 * time.Millisecond, Status: ptrace.StatusCodeOk, Attrs: map[string]any{"http.status_code": int64(500)}},
			}),
			want: Sampled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dec, err := sampler.Evaluate(traceID, tc.trace)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, dec)
		})
	}
}

func TestLinearModelSampler_BoundaryScoreEqualsThreshold_Sampled(t *testing.T) {
	// score = 1.0*duration_ms + 0*others
	weights := map[string]float64{
		"duration_ms": 1.0,
		"has_error":   0,
		"span_count":  0,
	}
	sampler := NewLinearModelSampler(zap.NewNop(), 100, 0, weights)

	now := time.Now()
	// duration 100ms => duration_ms=100 => score==threshold
	trace := newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
		{StartTime: now, Duration: 100 * time.Millisecond, Status: ptrace.StatusCodeOk, Attrs: nil},
	})

	dec, err := sampler.Evaluate(pcommon.TraceID([16]byte{1}), trace)
	assert.NoError(t, err)
	assert.Equal(t, Sampled, dec)
}

func TestLinearModelSampler_MultiSpanDuration_UsesEarliestToLatest(t *testing.T) {
	// duration_ms should be (latestEnd - earliestStart), not per-span duration.
	weights := map[string]float64{"duration_ms": 1.0}
	sampler := NewLinearModelSampler(zap.NewNop(), 500, 0, weights)

	now := time.Now()
	// Span A: [t0, t0+100ms]
	// Span B: starts later, ends at t0+500ms
	trace := newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
		{StartTime: now, Duration: 100 * time.Millisecond, Status: ptrace.StatusCodeOk, Attrs: nil},
		{StartTime: now.Add(200 * time.Millisecond), Duration: 300 * time.Millisecond, Status: ptrace.StatusCodeOk, Attrs: nil},
	})

	dec, err := sampler.Evaluate(pcommon.TraceID([16]byte{1}), trace)
	assert.NoError(t, err)
	assert.Equal(t, Sampled, dec)
}

func TestLinearModelSampler_InvalidScore_NaN_NotSampled(t *testing.T) {
	weights := map[string]float64{"duration_ms": math.NaN()}
	sampler := NewLinearModelSampler(zap.NewNop(), -1e9, 0, weights)

	now := time.Now()
	trace := newTraceWithSpansAndAttrs([]spanWithTimeAndDurationAndAttrs{
		{StartTime: now, Duration: 100 * time.Millisecond, Status: ptrace.StatusCodeOk, Attrs: nil},
	})

	dec, err := sampler.Evaluate(pcommon.TraceID([16]byte{1}), trace)
	assert.NoError(t, err)
	assert.Equal(t, NotSampled, dec)
}

type spanWithTimeAndDurationAndAttrs struct {
	StartTime time.Time
	Duration  time.Duration
	Status    ptrace.StatusCode
	Attrs     map[string]any
}

func newTraceWithSpansAndAttrs(spans []spanWithTimeAndDurationAndAttrs) *TraceData {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()

	spanCount := atomic.NewInt64(0)

	for _, s := range spans {
		span := ss.Spans().AppendEmpty()
		span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(s.StartTime))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(s.StartTime.Add(s.Duration)))
		span.Status().SetCode(s.Status)
		for k, v := range s.Attrs {
			switch vv := v.(type) {
			case int:
				span.Attributes().PutInt(k, int64(vv))
			case int64:
				span.Attributes().PutInt(k, vv)
			case bool:
				span.Attributes().PutBool(k, vv)
			case string:
				span.Attributes().PutStr(k, vv)
			}
		}
		spanCount.Inc()
	}

	return &TraceData{
		SpanCount:       spanCount,
		ReceivedBatches: traces,
	}
}
