package sampling

import (
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type linearModelSampler struct {
	logger    *zap.Logger
	threshold float64
	intercept float64
	weights   map[string]float64
}

var _ PolicyEvaluator = (*linearModelSampler)(nil)

// NewLinearModelSampler returns a policy evaluator that samples based on a simple linear score.
//
// Supported feature names:
// - duration_ms
// - span_count
// - has_error
func NewLinearModelSampler(logger *zap.Logger, threshold float64, intercept float64, weights map[string]float64) PolicyEvaluator {
	weightsCopy := map[string]float64{}
	for k, v := range weights {
		weightsCopy[k] = v
	}

	return &linearModelSampler{
		logger:    logger,
		threshold: threshold,
		intercept: intercept,
		weights:   weightsCopy,
	}
}

func (ms *linearModelSampler) Evaluate(_ pcommon.TraceID, trace *TraceData) (Decision, error) {
	trace.Lock()
	batches := trace.ReceivedBatches
	trace.Unlock()

	features := extractModelFeatures(batches, trace)
	score := ms.intercept
	for name, weight := range ms.weights {
		score += weight * features[name]
	}

	if math.IsNaN(score) || math.IsInf(score, 0) {
		ms.logger.Debug("model score invalid; not sampling")
		return NotSampled, nil
	}

	if score >= ms.threshold {
		return Sampled, nil
	}
	return NotSampled, nil
}

func extractModelFeatures(batches ptrace.Traces, trace *TraceData) map[string]float64 {

	// create default values
	features := map[string]float64{
		"duration_ms": 0,
		"span_count":  0,
		"has_error":   0,
	}

	if trace != nil && trace.SpanCount != nil {
		features["span_count"] = float64(trace.SpanCount.Load())
	}

	var (
		earliest pcommon.Timestamp
		latest   pcommon.Timestamp
		hasTime  bool
		hasError bool
	)

	// iterate through all spans to extract features
	// three layer of loops: ResourceSpans -> ScopeSpans -> Spans
	rs := batches.ResourceSpans()
	for i := 0; i < rs.Len(); i++ {
		ss := rs.At(i).ScopeSpans()
		for j := 0; j < ss.Len(); j++ {
			spans := ss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				st := span.StartTimestamp()
				et := span.EndTimestamp()
				if st != 0 && et != 0 {
					if !hasTime {
						earliest = st
						latest = et
						hasTime = true
					} else {
						if st < earliest {
							earliest = st
						}
						if et > latest {
							latest = et
						}
					}
				}

				if span.Status().Code() == ptrace.StatusCodeError {
					hasError = true
					continue
				}
				if v, ok := span.Attributes().Get("http.status_code"); ok {
					if v.Type() == pcommon.ValueTypeInt && v.Int() >= 500 {
						hasError = true
					}
				}
			}
		}
	}
	if hasTime && latest > earliest {
		features["duration_ms"] = float64(latest-earliest) / 1e6
	}
	if hasError {
		features["has_error"] = 1
	}
	return features
}
