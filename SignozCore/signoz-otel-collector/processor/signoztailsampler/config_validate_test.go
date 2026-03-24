package signoztailsampler

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Validate model policy configuration
func TestValidate_ModelPolicy_Valid(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{
			{
				BasePolicy: BasePolicy{
					Name:     "model-policy",
					Type:     Model,
					Priority: 1,
					ModelCfg: &ModelCfg{
						Type:      "linear",
						Threshold: 0.5,
						Intercept: 0.1,
						Weights: map[string]float64{
							"duration_ms": 1.0,
							"span_count":  0.25,
							"has_error":   10.0,
						},
					},
				},
			},
		},
	}

	require.NoError(t, cfg.Validate())
}

func TestValidate_ModelPolicy_AdaptiveValid(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{
			{
				BasePolicy: BasePolicy{
					Name:     "model-policy",
					Type:     Model,
					Priority: 1,
					ModelCfg: &ModelCfg{
						Type:      "linear",
						Threshold: 0.0,
						Intercept: 0.0,
						Weights: map[string]float64{
							"duration_ms": 1.0,
						},
						Adaptive: &ModelAdaptiveCfg{
							Enabled:            true,
							WindowDuration:     30 * time.Second,
							RecomputeInterval:  5 * time.Second,
							MaxSamples:         2048,
							TargetTracesPerSec: 10,
							AlwaysKeepErrors:   true,
						},
					},
				},
			},
		},
	}

	require.NoError(t, cfg.Validate())
}

func TestValidate_ModelPolicy_AdaptiveStateAwareValid(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{
			{
				BasePolicy: BasePolicy{
					Name:     "model-policy",
					Type:     Model,
					Priority: 1,
					ModelCfg: &ModelCfg{
						Type:      "linear",
						Threshold: 0.0,
						Intercept: 0.0,
						Weights: map[string]float64{
							"duration_ms": 1.0,
						},
						Adaptive: &ModelAdaptiveCfg{
							Enabled:            true,
							WindowDuration:     30 * time.Second,
							RecomputeInterval:  5 * time.Second,
							MaxSamples:         2048,
							TargetTracesPerSec: 10,
							AlwaysKeepErrors:   true,
							StateAware: &ModelAdaptiveStateAwareCfg{
								Enabled:                true,
								ErrorBurstThreshold:    0.2,
								ErrorBurstBoost:        0.15,
								LatencyTailQuantile:    0.99,
								LatencyTailThresholdMs: 300,
								LatencyTailBoost:       0.1,
							},
						},
					},
				},
			},
		},
	}

	require.NoError(t, cfg.Validate())
}

func TestValidate_ModelPolicy_AdaptiveStateAwareInvalid(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{
			{
				BasePolicy: BasePolicy{
					Name:     "model-policy",
					Type:     Model,
					Priority: 1,
					ModelCfg: &ModelCfg{
						Type:      "linear",
						Threshold: 0.0,
						Intercept: 0.0,
						Weights: map[string]float64{
							"duration_ms": 1.0,
						},
						Adaptive: &ModelAdaptiveCfg{
							Enabled:            true,
							WindowDuration:     30 * time.Second,
							RecomputeInterval:  5 * time.Second,
							MaxSamples:         2048,
							TargetTracesPerSec: 10,
							StateAware: &ModelAdaptiveStateAwareCfg{
								Enabled:                true,
								ErrorBurstThreshold:    1.2,
								ErrorBurstBoost:        -0.1,
								LatencyTailQuantile:    1.1,
								LatencyTailThresholdMs: -1,
								LatencyTailBoost:       1.5,
							},
						},
					},
				},
			},
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "model.adaptive.state_aware.error_burst_threshold")
	assert.Contains(t, err.Error(), "model.adaptive.state_aware.error_burst_boost")
	assert.Contains(t, err.Error(), "model.adaptive.state_aware.latency_tail_quantile")
	assert.Contains(t, err.Error(), "model.adaptive.state_aware.latency_tail_threshold_ms")
	assert.Contains(t, err.Error(), "model.adaptive.state_aware.latency_tail_boost")
}

func TestValidate_ModelPolicy_AdaptiveMissingTargets(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{
			{
				BasePolicy: BasePolicy{
					Name:     "model-policy",
					Type:     Model,
					Priority: 1,
					ModelCfg: &ModelCfg{
						Threshold: 0.0,
						Intercept: 0.0,
						Weights: map[string]float64{
							"duration_ms": 1.0,
						},
						Adaptive: &ModelAdaptiveCfg{
							Enabled:           true,
							WindowDuration:    30 * time.Second,
							RecomputeInterval: 5 * time.Second,
							MaxSamples:        100,
							// missing TargetTracesPerSec and KeepRatio
						},
					},
				},
			},
		},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "model.adaptive requires target_traces_per_sec")
}

// Validate model policy missing configuration
func TestValidate_ModelPolicy_MissingModelConfig(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{{
			BasePolicy: BasePolicy{
				Name:     "model-policy",
				Type:     Model,
				Priority: 1,
				ModelCfg: nil,
			},
		}},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "model config is required")
}

// Validate model policy with empty weights
func TestValidate_ModelPolicy_EmptyWeights(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{{
			BasePolicy: BasePolicy{
				Name:     "model-policy",
				Type:     Model,
				Priority: 1,
				ModelCfg: &ModelCfg{
					Threshold: 0.0,
					Intercept: 0.0,
					Weights:   map[string]float64{},
				},
			},
		}},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "model.weights must not be empty")
}

// Validate model policy with unknown feature in weights
func TestValidate_ModelPolicy_UnknownFeature(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{{
			BasePolicy: BasePolicy{
				Name:     "model-policy",
				Type:     Model,
				Priority: 1,
				ModelCfg: &ModelCfg{
					Threshold: 0.0,
					Intercept: 0.0,
					Weights: map[string]float64{
						"unknown": 1.0,
					},
				},
			},
		}},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported feature")
}

// Validate model policy with NaN data (with threshold)
func TestValidate_ModelPolicy_NaNThreshold(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{{
			BasePolicy: BasePolicy{
				Name:     "model-policy",
				Type:     Model,
				Priority: 1,
				ModelCfg: &ModelCfg{
					Threshold: math.NaN(),
					Intercept: 0.0,
					Weights: map[string]float64{
						"duration_ms": 1.0,
					},
				},
			},
		}},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "model.threshold must be a finite number")
}

// Validate model policy with infinite intercept
func TestValidate_SubPolicy_ModelValidation(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{{
			BasePolicy: BasePolicy{
				Name:     "group",
				Type:     PolicyGroup,
				Priority: 1,
			},
			SubPolicies: []BasePolicy{{
				Name:     "sub",
				Type:     Model,
				Priority: 1,
				ModelCfg: &ModelCfg{
					Threshold: 0,
					Intercept: 0,
					Weights:   map[string]float64{}},
			}},
		}},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "group/sub")
	assert.Contains(t, err.Error(), "model.weights must not be empty")
}

// Validate that priority must be set
func TestValidate_PriorityMustBeSet(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PolicyCfgs: []PolicyGroupCfg{{
			BasePolicy: BasePolicy{
				Name: "missing-priority",
				Type: PolicyGroup,
			},
		}},
	}

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "priority must be greater than 0")
}
