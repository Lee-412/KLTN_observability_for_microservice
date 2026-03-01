package sampling

import (
	"math"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// AdaptiveLinearModelSamplerConfig configures adaptive thresholding for the linear model sampler.
//
// The sampler keeps a sliding set of recent scores (MaxSamples) and periodically recomputes
// a threshold based on a score quantile. The target keep fraction is derived either from a
// trace budget (TargetTracesPerSec / estimated incoming rate) or a fixed KeepRatio.
//
// NOTE: This is best-effort. Exact keep-rate is not guaranteed (ties, score distribution drift).
//
// TimeNow is intended for tests; leave nil in production.
type AdaptiveLinearModelSamplerConfig struct {
	FallbackThreshold float64
	Intercept         float64
	Weights           map[string]float64

	WindowDuration    time.Duration
	RecomputeInterval time.Duration
	MaxSamples        int

	TargetTracesPerSec float64
	KeepRatio          float64
	MinKeepRatio       float64
	MaxKeepRatio       float64

	AlwaysKeepErrors bool

	SlaDurationMs          float64
	ViolationRateThreshold float64
	IncidentKeepRatio      float64

	TimeNow func() time.Time
}

type adaptiveLinearModelSampler struct {
	logger *zap.Logger

	fallbackThreshold float64
	intercept         float64
	weights           map[string]float64

	windowDuration    time.Duration
	recomputeInterval time.Duration
	maxSamples        int

	targetTracesPerSec float64
	keepRatio          float64
	minKeepRatio       float64
	maxKeepRatio       float64

	alwaysKeepErrors bool

	slaDurationMs          float64
	violationRateThreshold float64
	incidentKeepRatio      float64

	now func() time.Time

	mu                   sync.Mutex
	scores               []float64
	scoresFilled         bool
	scoresNextIdx        int
	windowStart          time.Time
	windowTraceCount     int64
	windowViolationCount int64
	lastRecompute        time.Time
	currentThreshold     float64
	currentKeepRatio     float64
	firstRecomputeLogged bool
}

var _ PolicyEvaluator = (*adaptiveLinearModelSampler)(nil)

func NewAdaptiveLinearModelSampler(logger *zap.Logger, cfg AdaptiveLinearModelSamplerConfig) PolicyEvaluator {
	weightsCopy := map[string]float64{}
	for k, v := range cfg.Weights {
		weightsCopy[k] = v
	}

	nowFn := cfg.TimeNow
	if nowFn == nil {
		nowFn = time.Now
	}

	maxKeep := cfg.MaxKeepRatio
	if maxKeep == 0 {
		maxKeep = 1.0
	}

	ms := &adaptiveLinearModelSampler{
		logger:                 logger,
		fallbackThreshold:      cfg.FallbackThreshold,
		intercept:              cfg.Intercept,
		weights:                weightsCopy,
		windowDuration:         cfg.WindowDuration,
		recomputeInterval:      cfg.RecomputeInterval,
		maxSamples:             cfg.MaxSamples,
		targetTracesPerSec:     cfg.TargetTracesPerSec,
		keepRatio:              cfg.KeepRatio,
		minKeepRatio:           cfg.MinKeepRatio,
		maxKeepRatio:           maxKeep,
		alwaysKeepErrors:       cfg.AlwaysKeepErrors,
		slaDurationMs:          cfg.SlaDurationMs,
		violationRateThreshold: cfg.ViolationRateThreshold,
		incidentKeepRatio:      cfg.IncidentKeepRatio,
		now:                    nowFn,
		currentThreshold:       cfg.FallbackThreshold,
		currentKeepRatio:       1.0,
	}

	logger.Info(
		"adaptive model sampler initialized",
		zap.Float64("fallback_threshold", cfg.FallbackThreshold),
		zap.Duration("window_duration", cfg.WindowDuration),
		zap.Duration("recompute_interval", cfg.RecomputeInterval),
		zap.Int("max_samples", cfg.MaxSamples),
		zap.Float64("target_traces_per_sec", cfg.TargetTracesPerSec),
		zap.Float64("keep_ratio", cfg.KeepRatio),
		zap.Float64("min_keep_ratio", cfg.MinKeepRatio),
		zap.Float64("max_keep_ratio", maxKeep),
		zap.Bool("always_keep_errors", cfg.AlwaysKeepErrors),
	)

	// Initialize the rate window on first observed trace. If we start the window at
	// process startup and the collector is idle for a while, the first recompute can
	// estimate an artificially low incoming rate and clamp keepRatio to 1 (keep all)
	// until the next recompute interval.
	ms.windowStart = time.Time{}
	ms.lastRecompute = time.Time{}
	ms.scores = make([]float64, 0, cfg.MaxSamples)
	return ms
}

func (ms *adaptiveLinearModelSampler) Evaluate(_ pcommon.TraceID, trace *TraceData) (Decision, error) {
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

	hasError := features["has_error"] >= 1
	durationMs := features["duration_ms"]

	now := ms.now()

	ms.mu.Lock()
	// Rotate the rate window on trace arrival so the current trace contributes to the
	// new window (and recompute uses a meaningful elapsed duration).
	if ms.windowStart.IsZero() {
		ms.windowStart = now
	} else if ms.windowDuration > 0 && now.Sub(ms.windowStart) >= ms.windowDuration {
		ms.windowStart = now
		ms.windowTraceCount = 0
		ms.windowViolationCount = 0
	}

	ms.windowTraceCount++
	if hasError || (ms.slaDurationMs > 0 && durationMs >= ms.slaDurationMs) {
		ms.windowViolationCount++
	}
	ms.addScoreLocked(score)

	// recompute threshold periodically.
	if ms.lastRecompute.IsZero() || now.Sub(ms.lastRecompute) >= ms.recomputeInterval {
		ms.recomputeLocked(now)
	}
	threshold := ms.currentThreshold
	alwaysKeepErrors := ms.alwaysKeepErrors
	ms.mu.Unlock()

	if alwaysKeepErrors && hasError {
		return Sampled, nil
	}

	if score >= threshold {
		return Sampled, nil
	}
	return NotSampled, nil
}

func (ms *adaptiveLinearModelSampler) addScoreLocked(score float64) {
	if ms.maxSamples <= 0 {
		return
	}

	if len(ms.scores) < ms.maxSamples {
		ms.scores = append(ms.scores, score)
		return
	}

	// Ring-buffer overwrite (last-N scores).
	ms.scores[ms.scoresNextIdx] = score
	ms.scoresNextIdx++
	if ms.scoresNextIdx >= ms.maxSamples {
		ms.scoresNextIdx = 0
		ms.scoresFilled = true
	}
}

func (ms *adaptiveLinearModelSampler) recomputeLocked(now time.Time) {
	elapsed := now.Sub(ms.windowStart)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}

	incomingRate := float64(ms.windowTraceCount) / elapsed.Seconds()
	keepRatio := ms.targetKeepRatioLocked(incomingRate)
	baseKeepRatio := keepRatio

	// SLA incident boost.
	incidentBoosted := false
	if ms.incidentKeepRatio > 0 {
		var violationRate float64
		if ms.windowTraceCount > 0 {
			violationRate = float64(ms.windowViolationCount) / float64(ms.windowTraceCount)
		}
		thr := ms.violationRateThreshold
		incident := false
		if thr <= 0 {
			incident = ms.windowViolationCount > 0
		} else {
			incident = violationRate >= thr
		}
		if incident {
			if ms.incidentKeepRatio > keepRatio {
				keepRatio = ms.incidentKeepRatio
				incidentBoosted = true
			}
		}
	}

	keepRatio = clamp01(keepRatio)
	if ms.minKeepRatio > 0 {
		keepRatio = math.Max(keepRatio, ms.minKeepRatio)
	}
	if ms.maxKeepRatio > 0 {
		keepRatio = math.Min(keepRatio, ms.maxKeepRatio)
	}

	threshold := ms.fallbackThreshold
	if keepRatio >= 1 {
		threshold = math.Inf(-1)
	} else if keepRatio <= 0 {
		threshold = math.Inf(1)
	} else if len(ms.scores) > 0 {
		q := 1.0 - keepRatio
		threshold = quantile(ms.scores, q)
	}

	ms.currentThreshold = threshold
	ms.currentKeepRatio = keepRatio
	ms.lastRecompute = now

	if !ms.firstRecomputeLogged {
		ms.firstRecomputeLogged = true
		ms.logger.Info(
			"adaptive model sampler first recompute",
			zap.Float64("incoming_rate", incomingRate),
			zap.Int64("window_trace_count", ms.windowTraceCount),
			zap.Int64("window_violation_count", ms.windowViolationCount),
			zap.Float64("target_traces_per_sec", ms.targetTracesPerSec),
			zap.Float64("base_keep_ratio", baseKeepRatio),
			zap.Float64("keep_ratio", keepRatio),
			zap.Bool("incident_boosted", incidentBoosted),
			zap.Float64("threshold", threshold),
			zap.Int("scores_len", len(ms.scores)),
			zap.Duration("window_elapsed", elapsed),
		)
	}

	ms.logger.Debug(
		"adaptive model sampler recompute",
		zap.Float64("incoming_rate", incomingRate),
		zap.Int64("window_trace_count", ms.windowTraceCount),
		zap.Int64("window_violation_count", ms.windowViolationCount),
		zap.Float64("target_traces_per_sec", ms.targetTracesPerSec),
		zap.Float64("base_keep_ratio", baseKeepRatio),
		zap.Float64("keep_ratio", keepRatio),
		zap.Bool("incident_boosted", incidentBoosted),
		zap.Float64("threshold", threshold),
		zap.Int("scores_len", len(ms.scores)),
		zap.Duration("window_elapsed", elapsed),
	)
}

func (ms *adaptiveLinearModelSampler) targetKeepRatioLocked(incomingRate float64) float64 {
	if ms.targetTracesPerSec > 0 {
		if incomingRate <= 0 {
			return 1.0
		}
		return ms.targetTracesPerSec / incomingRate
	}
	if ms.keepRatio > 0 {
		return ms.keepRatio
	}
	return 1.0
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// quantile returns an approximate quantile from the provided sample.
// q in [0,1].
func quantile(sample []float64, q float64) float64 {
	if len(sample) == 0 {
		return math.NaN()
	}
	if q <= 0 {
		min := sample[0]
		for _, v := range sample[1:] {
			if v < min {
				min = v
			}
		}
		return min
	}
	if q >= 1 {
		max := sample[0]
		for _, v := range sample[1:] {
			if v > max {
				max = v
			}
		}
		return max
	}

	cpy := make([]float64, len(sample))
	copy(cpy, sample)
	sort.Float64s(cpy)

	// Index at quantile.
	pos := q * float64(len(cpy)-1)
	idx := int(math.Ceil(pos))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cpy) {
		idx = len(cpy) - 1
	}
	return cpy[idx]
}

// extractModelFeatures is referenced here to keep feature computation consistent.
// (This file sits in the same package, so we can reuse it.)
func _adaptiveModelSamplerCompileGuard(_ ptrace.Traces) {}
