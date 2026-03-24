package sampling

import (
	"encoding/binary"
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

	WindowDuration      time.Duration
	DualWindowEnabled   bool
	ShortWindowDuration time.Duration
	LongWindowDuration  time.Duration
	AlphaNormal         float64
	AlphaIncident       float64
	RecomputeInterval   time.Duration
	MaxSamples          int

	TargetTracesPerSec float64
	KeepRatio          float64
	MinKeepRatio       float64
	MaxKeepRatio       float64

	AlwaysKeepErrors bool

	SlaDurationMs          float64
	ViolationRateThreshold float64
	IncidentKeepRatio      float64

	StateAwareEnabled   bool
	ErrorBurstThreshold float64
	ErrorBurstBoost     float64

	LatencyTailQuantile    float64
	LatencyTailThresholdMs float64
	LatencyTailBoost       float64

	TimeNow func() time.Time
}

type traceRuntimeSample struct {
	ts         time.Time
	score      float64
	hasError   bool
	durationMs float64
}

type adaptiveLinearModelSampler struct {
	logger *zap.Logger

	fallbackThreshold float64
	intercept         float64
	weights           map[string]float64

	windowDuration      time.Duration
	dualWindowEnabled   bool
	shortWindowDuration time.Duration
	longWindowDuration  time.Duration
	alphaNormal         float64
	alphaIncident       float64
	recomputeInterval   time.Duration
	maxSamples          int

	targetTracesPerSec float64
	keepRatio          float64
	minKeepRatio       float64
	maxKeepRatio       float64

	alwaysKeepErrors bool

	slaDurationMs          float64
	violationRateThreshold float64
	incidentKeepRatio      float64

	stateAwareEnabled   bool
	errorBurstThreshold float64
	errorBurstBoost     float64

	latencyTailQuantile    float64
	latencyTailThresholdMs float64
	latencyTailBoost       float64

	now func() time.Time

	mu                   sync.Mutex
	samples              []traceRuntimeSample
	samplesFilled        bool
	samplesNextIdx       int
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

	latencyTailQuantile := cfg.LatencyTailQuantile
	if latencyTailQuantile <= 0 {
		latencyTailQuantile = 0.99
	}

	shortWindowDuration := cfg.ShortWindowDuration
	if shortWindowDuration <= 0 {
		shortWindowDuration = cfg.WindowDuration
	}
	longWindowDuration := cfg.LongWindowDuration
	if longWindowDuration <= 0 {
		longWindowDuration = cfg.WindowDuration
	}

	ms := &adaptiveLinearModelSampler{
		logger:                 logger,
		fallbackThreshold:      cfg.FallbackThreshold,
		intercept:              cfg.Intercept,
		weights:                weightsCopy,
		windowDuration:         cfg.WindowDuration,
		dualWindowEnabled:      cfg.DualWindowEnabled,
		shortWindowDuration:    shortWindowDuration,
		longWindowDuration:     longWindowDuration,
		alphaNormal:            clamp01(cfg.AlphaNormal),
		alphaIncident:          clamp01(cfg.AlphaIncident),
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
		stateAwareEnabled:      cfg.StateAwareEnabled,
		errorBurstThreshold:    cfg.ErrorBurstThreshold,
		errorBurstBoost:        cfg.ErrorBurstBoost,
		latencyTailQuantile:    clamp01(latencyTailQuantile),
		latencyTailThresholdMs: cfg.LatencyTailThresholdMs,
		latencyTailBoost:       cfg.LatencyTailBoost,
		now:                    nowFn,
		currentThreshold:       cfg.FallbackThreshold,
		currentKeepRatio:       1.0,
	}

	logger.Info(
		"adaptive model sampler initialized",
		zap.Float64("fallback_threshold", cfg.FallbackThreshold),
		zap.Duration("window_duration", cfg.WindowDuration),
		zap.Bool("dual_window_enabled", cfg.DualWindowEnabled),
		zap.Duration("short_window_duration", shortWindowDuration),
		zap.Duration("long_window_duration", longWindowDuration),
		zap.Float64("alpha_normal", clamp01(cfg.AlphaNormal)),
		zap.Float64("alpha_incident", clamp01(cfg.AlphaIncident)),
		zap.Duration("recompute_interval", cfg.RecomputeInterval),
		zap.Int("max_samples", cfg.MaxSamples),
		zap.Float64("target_traces_per_sec", cfg.TargetTracesPerSec),
		zap.Float64("keep_ratio", cfg.KeepRatio),
		zap.Float64("min_keep_ratio", cfg.MinKeepRatio),
		zap.Float64("max_keep_ratio", maxKeep),
		zap.Bool("always_keep_errors", cfg.AlwaysKeepErrors),
		zap.Bool("state_aware_enabled", cfg.StateAwareEnabled),
		zap.Float64("state_error_burst_threshold", cfg.ErrorBurstThreshold),
		zap.Float64("state_error_burst_boost", cfg.ErrorBurstBoost),
		zap.Float64("state_latency_tail_quantile", clamp01(latencyTailQuantile)),
		zap.Float64("state_latency_tail_threshold_ms", cfg.LatencyTailThresholdMs),
		zap.Float64("state_latency_tail_boost", cfg.LatencyTailBoost),
	)

	// Initialize the rate window on first observed trace. If we start the window at
	// process startup and the collector is idle for a while, the first recompute can
	// estimate an artificially low incoming rate and clamp keepRatio to 1 (keep all)
	// until the next recompute interval.
	ms.windowStart = time.Time{}
	ms.lastRecompute = time.Time{}
	ms.samples = make([]traceRuntimeSample, 0, cfg.MaxSamples)
	return ms
}

func (ms *adaptiveLinearModelSampler) Evaluate(traceID pcommon.TraceID, trace *TraceData) (Decision, error) {
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
	ms.addTraceSampleLocked(now, score, hasError, durationMs)

	// recompute threshold periodically.
	if ms.lastRecompute.IsZero() || now.Sub(ms.lastRecompute) >= ms.recomputeInterval {
		ms.recomputeLocked(now)
	}
	threshold := ms.currentThreshold
	keepRatio := ms.currentKeepRatio
	alwaysKeepErrors := ms.alwaysKeepErrors
	ms.mu.Unlock()

	if alwaysKeepErrors && hasError {
		return Sampled, nil
	}

	if score > threshold {
		return Sampled, nil
	}
	if score < threshold {
		return NotSampled, nil
	}

	// Tie-break when score == threshold.
	// If many traces share the same score (common with discrete features), using
	// `score >= threshold` would keep ~100% at the boundary. We instead sample a
	// deterministic fraction based on traceID.
	if keepRatio <= 0 {
		return NotSampled, nil
	}
	if keepRatio >= 1 {
		return Sampled, nil
	}
	if shouldKeepOnTie(traceID, keepRatio) {
		return Sampled, nil
	}
	return NotSampled, nil
}

func shouldKeepOnTie(traceID pcommon.TraceID, keepRatio float64) bool {
	// Use a stable hash from the traceID bytes (no RNG).
	// XOR the two uint64 halves of the 16-byte TraceID.
	b := traceID[:]
	lo := binary.LittleEndian.Uint64(b[0:8])
	hi := binary.LittleEndian.Uint64(b[8:16])
	id := lo ^ hi

	// Map keepRatio to [0, MaxUint64].
	max := ^uint64(0)
	cutoff := uint64(keepRatio * float64(max))
	return id <= cutoff
}

func (ms *adaptiveLinearModelSampler) addTraceSampleLocked(now time.Time, score float64, hasError bool, durationMs float64) {
	if ms.maxSamples <= 0 {
		return
	}

	entry := traceRuntimeSample{ts: now, score: score, hasError: hasError, durationMs: durationMs}

	if len(ms.samples) < ms.maxSamples {
		ms.samples = append(ms.samples, entry)
		return
	}

	// Ring-buffer overwrite (last-N scores).
	ms.samples[ms.samplesNextIdx] = entry
	ms.samplesNextIdx++
	if ms.samplesNextIdx >= ms.maxSamples {
		ms.samplesNextIdx = 0
		ms.samplesFilled = true
	}
}

func (ms *adaptiveLinearModelSampler) collectScoresLocked(now time.Time) ([]float64, []float64) {
	if len(ms.samples) == 0 {
		return nil, nil
	}

	all := make([]float64, 0, len(ms.samples))
	shortScores := make([]float64, 0, len(ms.samples))

	shortCutoff := now.Add(-ms.shortWindowDuration)
	longCutoff := now.Add(-ms.longWindowDuration)

	for _, s := range ms.samples {
		if ms.dualWindowEnabled && ms.longWindowDuration > 0 && s.ts.Before(longCutoff) {
			continue
		}
		all = append(all, s.score)
		if ms.shortWindowDuration <= 0 || !s.ts.Before(shortCutoff) {
			shortScores = append(shortScores, s.score)
		}
	}

	return all, shortScores
}

func (ms *adaptiveLinearModelSampler) runtimeStateBoostLocked(now time.Time) (float64, float64, float64, float64, bool, bool) {
	if !ms.stateAwareEnabled {
		return 0, 0, 0, 0, false, false
	}

	cutoff := now.Add(-ms.windowDuration)
	errorCount := 0
	totalCount := 0
	durations := make([]float64, 0, len(ms.samples))

	for _, s := range ms.samples {
		if ms.windowDuration > 0 && s.ts.Before(cutoff) {
			continue
		}
		totalCount++
		if s.hasError {
			errorCount++
		}
		if s.durationMs > 0 {
			durations = append(durations, s.durationMs)
		}
	}

	errorRate := 0.0
	if totalCount > 0 {
		errorRate = float64(errorCount) / float64(totalCount)
	}

	latencyTailMs := 0.0
	if len(durations) > 0 {
		latencyTailMs = quantile(durations, clamp01(ms.latencyTailQuantile))
	}

	errorBurstActive := ms.errorBurstBoost > 0 && ms.errorBurstThreshold > 0 && errorRate >= ms.errorBurstThreshold
	latencyTailActive := ms.latencyTailBoost > 0 && ms.latencyTailThresholdMs > 0 && latencyTailMs >= ms.latencyTailThresholdMs

	boost := 0.0
	if errorBurstActive {
		boost += ms.errorBurstBoost
	}
	if latencyTailActive {
		boost += ms.latencyTailBoost
	}

	return boost, errorRate, latencyTailMs, ms.latencyTailQuantile, errorBurstActive, latencyTailActive
}

func (ms *adaptiveLinearModelSampler) recomputeLocked(now time.Time) {
	elapsed := now.Sub(ms.windowStart)
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}

	incomingRate := float64(ms.windowTraceCount) / elapsed.Seconds()
	keepRatio := ms.targetKeepRatioLocked(incomingRate)
	baseKeepRatio := keepRatio
	stateBoost, runtimeErrorRate, runtimeLatencyTailMs, runtimeLatencyQuantile, errorBurstActive, latencyTailActive := ms.runtimeStateBoostLocked(now)
	if stateBoost > 0 {
		keepRatio += stateBoost
	}
	incident := false

	// SLA incident boost.
	incidentBoosted := false
	if ms.incidentKeepRatio > 0 {
		var violationRate float64
		if ms.windowTraceCount > 0 {
			violationRate = float64(ms.windowViolationCount) / float64(ms.windowTraceCount)
		}
		thr := ms.violationRateThreshold
		incident = false
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
	} else {
		q := 1.0 - keepRatio
		longScores, shortScores := ms.collectScoresLocked(now)
		if len(longScores) > 0 {
			if ms.dualWindowEnabled {
				thrLong := quantile(longScores, q)
				thrShort := thrLong
				if len(shortScores) > 0 {
					thrShort = quantile(shortScores, q)
				}

				alpha := ms.alphaNormal
				if incident {
					alpha = ms.alphaIncident
				}
				alpha = clamp01(alpha)
				threshold = alpha*thrShort + (1.0-alpha)*thrLong
			} else {
				threshold = quantile(longScores, q)
			}
		}
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
			zap.Float64("state_boost", stateBoost),
			zap.Float64("runtime_error_rate", runtimeErrorRate),
			zap.Float64("runtime_latency_tail_ms", runtimeLatencyTailMs),
			zap.Float64("runtime_latency_tail_quantile", runtimeLatencyQuantile),
			zap.Bool("state_error_burst_active", errorBurstActive),
			zap.Bool("state_latency_tail_active", latencyTailActive),
			zap.Float64("keep_ratio", keepRatio),
			zap.Bool("incident_boosted", incidentBoosted),
			zap.Bool("incident_state", incident),
			zap.Float64("threshold", threshold),
			zap.Bool("dual_window_enabled", ms.dualWindowEnabled),
			zap.Int("scores_len", len(ms.samples)),
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
		zap.Float64("state_boost", stateBoost),
		zap.Float64("runtime_error_rate", runtimeErrorRate),
		zap.Float64("runtime_latency_tail_ms", runtimeLatencyTailMs),
		zap.Float64("runtime_latency_tail_quantile", runtimeLatencyQuantile),
		zap.Bool("state_error_burst_active", errorBurstActive),
		zap.Bool("state_latency_tail_active", latencyTailActive),
		zap.Float64("keep_ratio", keepRatio),
		zap.Bool("incident_boosted", incidentBoosted),
		zap.Bool("incident_state", incident),
		zap.Float64("threshold", threshold),
		zap.Bool("dual_window_enabled", ms.dualWindowEnabled),
		zap.Int("scores_len", len(ms.samples)),
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
