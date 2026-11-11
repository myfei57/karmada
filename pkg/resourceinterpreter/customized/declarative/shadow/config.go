/*
Copyright 2026 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package shadow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/yaml"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
)

const (
	// defaultQueueCapacity bounds the number of shadow tasks waiting for a worker.
	defaultQueueCapacity = 128
	// defaultWorkers is the fixed size of the shadow worker pool.
	defaultWorkers = 2
	// defaultSamplingRate evaluates every admitted input once the feature is enabled.
	defaultSamplingRate = 1.0
	// defaultMaxInputBytes rejects single tasks whose serialized input exceeds 256KiB.
	defaultMaxInputBytes = 256 * 1024
	// defaultMaxQueuedInputBytes bounds the total serialized input held by queued and
	// in-flight tasks to 8MiB.
	defaultMaxQueuedInputBytes int64 = 8 * 1024 * 1024
	// samplingDenominator gives a 1/10000 resolution to the sampling decision.
	samplingDenominator uint64 = 10000
	// digestLength is the retained prefix length of a sha256 digest.
	digestLength = 12
)

// defaultExecutionTimeout bounds a single candidate script execution.
var defaultExecutionTimeout = metav1.Duration{Duration: time.Second}

// Config declares the optional shadow behavior. The zero value disables shadow
// interpretation; the active interpretation path is never affected by this config.
type Config struct {
	// Enabled gates the whole feature. It defaults to false so that shadow
	// interpretation can never change runtime behavior without an explicit opt-in.
	// +optional
	Enabled bool `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	// QueueCapacity bounds the number of pending shadow tasks. When the queue is
	// full, new submissions are recorded as Skipped(queue_full) instead of blocking.
	// +optional
	QueueCapacity int `json:"queueCapacity,omitempty" yaml:"queueCapacity,omitempty"`
	// Workers is the fixed number of goroutines consuming shadow tasks. The number
	// of concurrently running candidate scripts is therefore always bounded.
	// +optional
	Workers int `json:"workers,omitempty" yaml:"workers,omitempty"`
	// ExecutionTimeout bounds a single candidate script execution.
	// +optional
	ExecutionTimeout metav1.Duration `json:"executionTimeout,omitempty" yaml:"executionTimeout,omitempty"`
	// SamplingRate is in the closed interval [0.0, 1.0]. The decision is a stable
	// function of operation/resource UID/object generation, so the same input is
	// either always sampled or never sampled while the configuration is unchanged.
	// +optional
	SamplingRate float64 `json:"samplingRate,omitempty" yaml:"samplingRate,omitempty"`
	// MaxInputBytes rejects a single task whose serialized inputs exceed the budget.
	// +optional
	MaxInputBytes int `json:"maxInputBytes,omitempty" yaml:"maxInputBytes,omitempty"`
	// MaxQueuedInputBytes bounds the total serialized input bytes of queued and
	// in-flight tasks, providing a global memory budget.
	// +optional
	MaxQueuedInputBytes int64 `json:"maxQueuedInputBytes,omitempty" yaml:"maxQueuedInputBytes,omitempty"`
	// Rules declares candidate scripts for explicitly allowed local Lua operations.
	// Webhook and external interpreters with side effects are never shadowed.
	// +optional
	Rules []Rule `json:"rules,omitempty" yaml:"rules,omitempty"`
}

// Rule binds a candidate Lua script to one resource type and one allowed operation.
type Rule struct {
	// APIVersion and Kind identify the target resource, mirroring
	// ResourceInterpreterCustomization.spec.target.
	APIVersion string `json:"apiVersion" yaml:"apiVersion"`
	Kind       string `json:"kind" yaml:"kind"`
	// Operation is one of GetReplicas, InterpretHealth or ReviseReplica.
	Operation string `json:"operation" yaml:"operation"`
	// CandidateScript holds the candidate Lua script. Its source is never logged or
	// exposed in diagnostics; only its length and content hash are retained.
	CandidateScript string `json:"candidateScript" yaml:"candidateScript"`
	// ScriptVersion is an optional opaque version. When empty, a short sha256 content
	// hash is computed so that script replacements are always distinguishable.
	// +optional
	ScriptVersion string `json:"scriptVersion,omitempty" yaml:"scriptVersion,omitempty"`
	// AllowedDiffFields declares JSON field paths (e.g. "spec.replicas") where
	// ReviseReplica differences are tolerated. Protected identity fields can never
	// be allowed. It is rejected for non-ReviseReplica operations.
	// +optional
	AllowedDiffFields []string `json:"allowedDiffFields,omitempty" yaml:"allowedDiffFields,omitempty"`
}

// ruleKey indexes compiled rules by GVK and operation.
type ruleKey struct {
	gvk       schema.GroupVersionKind
	operation configv1alpha1.InterpreterOperation
}

// compiledRule is the immutable internal representation of a validated Rule.
type compiledRule struct {
	script        string
	version       string
	allowedFields sets.Set[string]
}

// snapshot is an immutable view of a configuration generation. Replacing a snapshot
// bumps the generation; in-flight tasks keep referencing the old snapshot through
// their captured rule and generation number.
type snapshot struct {
	generation int64
	config     *Config
	rules      map[ruleKey]*compiledRule
}

// allowedOperations is the closed set of operations eligible for shadow execution.
// Only side-effect-free local Lua customizations participate: webhook and other
// external interpreters are deliberately excluded.
var allowedOperations = map[configv1alpha1.InterpreterOperation]string{
	configv1alpha1.InterpreterOperationInterpretReplica: "GetReplicas",
	configv1alpha1.InterpreterOperationInterpretHealth:  "InterpretHealth",
	configv1alpha1.InterpreterOperationReviseReplica:    "ReviseReplica",
}

// SetDefaults fills unset fields with safe defaults.
func (c *Config) SetDefaults() {
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = defaultQueueCapacity
	}
	if c.Workers <= 0 {
		c.Workers = defaultWorkers
	}
	if c.ExecutionTimeout.Duration <= 0 {
		c.ExecutionTimeout = defaultExecutionTimeout
	}
	if c.SamplingRate == 0 {
		c.SamplingRate = defaultSamplingRate
	}
	if c.MaxInputBytes <= 0 {
		c.MaxInputBytes = defaultMaxInputBytes
	}
	if c.MaxQueuedInputBytes <= 0 {
		c.MaxQueuedInputBytes = defaultMaxQueuedInputBytes
	}
}

// Validate checks the configuration and returns the first encountered error.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if c.SamplingRate < 0 || c.SamplingRate > 1 {
		return fmt.Errorf("samplingRate must be within [0.0, 1.0], got %v", c.SamplingRate)
	}
	if c.QueueCapacity < 0 {
		return fmt.Errorf("queueCapacity must not be negative, got %d", c.QueueCapacity)
	}
	if c.Workers < 0 {
		return fmt.Errorf("workers must not be negative, got %d", c.Workers)
	}
	if c.ExecutionTimeout.Duration < 0 {
		return fmt.Errorf("executionTimeout must not be negative, got %v", c.ExecutionTimeout.Duration)
	}
	if c.MaxInputBytes < 0 {
		return fmt.Errorf("maxInputBytes must not be negative, got %d", c.MaxInputBytes)
	}
	if c.MaxQueuedInputBytes < 0 {
		return fmt.Errorf("maxQueuedInputBytes must not be negative, got %d", c.MaxQueuedInputBytes)
	}

	seen := sets.New[ruleKey]()
	for i := range c.Rules {
		rule := &c.Rules[i]
		op, ok := parseAllowedOperation(rule.Operation)
		if !ok {
			return fmt.Errorf("rules[%d]: operation %q is not allowed for shadow interpretation, allowed: GetReplicas, InterpretHealth, ReviseReplica", i, rule.Operation)
		}
		if rule.CandidateScript == "" {
			return fmt.Errorf("rules[%d]: candidateScript must not be empty", i)
		}
		gvk, err := schema.ParseGroupVersion(rule.APIVersion)
		if err != nil {
			return fmt.Errorf("rules[%d]: invalid apiVersion %q: %w", i, rule.APIVersion, err)
		}
		if rule.Kind == "" {
			return fmt.Errorf("rules[%d]: kind must not be empty", i)
		}
		key := ruleKey{gvk: gvk.WithKind(rule.Kind), operation: op}
		if seen.Has(key) {
			return fmt.Errorf("rules[%d]: duplicate shadow rule for %s/%s %s", i, rule.APIVersion, rule.Kind, rule.Operation)
		}
		seen.Insert(key)

		if op != configv1alpha1.InterpreterOperationReviseReplica && len(rule.AllowedDiffFields) > 0 {
			return fmt.Errorf("rules[%d]: allowedDiffFields is only valid for ReviseReplica, not %s", i, rule.Operation)
		}
	}
	return nil
}

// compile produces an immutable snapshot with the given monotonically increasing
// generation. Nil or disabled configurations compile to an empty snapshot.
// Enabled configurations are not defaulted: file loaders apply SetDefaults first,
// while programmatic callers may set every field explicitly, including an explicit
// zero SamplingRate to disable sampling.
func (c *Config) compile(generation int64) (*snapshot, error) {
	if c == nil || !c.Enabled {
		disabled := &Config{}
		if c != nil {
			disabled = c
		}
		// Structural defaults are still applied on the disabled path so that fields
		// such as queue capacity stay well-defined; the snapshot remains inert because
		// admission rejects every task while Enabled is false.
		disabled.SetDefaults()
		disabled.Enabled = false
		return &snapshot{generation: generation, config: disabled, rules: map[ruleKey]*compiledRule{}}, nil
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}

	rules := make(map[ruleKey]*compiledRule, len(c.Rules))
	for i := range c.Rules {
		rule := &c.Rules[i]
		op, _ := parseAllowedOperation(rule.Operation)
		gv, _ := schema.ParseGroupVersion(rule.APIVersion)
		version := rule.ScriptVersion
		if version == "" {
			version = shortDigest(rule.CandidateScript)
		}
		rules[ruleKey{gvk: gv.WithKind(rule.Kind), operation: op}] = &compiledRule{
			script:        rule.CandidateScript,
			version:       version,
			allowedFields: sets.New(rule.AllowedDiffFields...),
		}
	}
	return &snapshot{generation: generation, config: c, rules: rules}, nil
}

// parseAllowedOperation maps the external operation name to the API enum.
func parseAllowedOperation(operation string) (configv1alpha1.InterpreterOperation, bool) {
	for op, name := range allowedOperations {
		if name == operation {
			return op, true
		}
	}
	return "", false
}

// LoadConfigFromFile reads, parses and validates a shadow configuration YAML file.
// An empty path yields a disabled configuration so callers can wire the feature
// unconditionally behind a flag.
func LoadConfigFromFile(path string) (*Config, error) {
	if path == "" {
		return &Config{Enabled: false}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read shadow resource interpreter config %q: %w", path, err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse shadow resource interpreter config %q: %w", path, err)
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid shadow resource interpreter config %q: %w", path, err)
	}
	return cfg, nil
}

// shortDigest returns a bounded-length sha256 hex digest of the given content.
func shortDigest(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])[:digestLength]
}
