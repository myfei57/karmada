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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
)

const (
	testAPIVersion = "example.io/v1"
	testKind       = "Workload"
)

// diagnosticSink collects shadow diagnostics in memory for assertions.
type diagnosticSink struct {
	mu    sync.Mutex
	items []Diagnostic
}

func (s *diagnosticSink) collect(diagnostic Diagnostic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, diagnostic)
}

func (s *diagnosticSink) all() []Diagnostic {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]Diagnostic, len(s.items))
	copy(copied, s.items)
	return copied
}

func (s *diagnosticSink) waitForCount(t *testing.T, count int, timeout time.Duration) []Diagnostic {
	t.Helper()
	require.Eventually(t, func() bool {
		return len(s.all()) >= count
	}, timeout, 5*time.Millisecond)
	return s.all()
}

func (s *diagnosticSink) waitForOutcome(t *testing.T, outcome Outcome, timeout time.Duration) Diagnostic {
	t.Helper()
	var found Diagnostic
	require.Eventually(t, func() bool {
		for _, diagnostic := range s.all() {
			if diagnostic.Outcome == outcome {
				found = diagnostic
				return true
			}
		}
		return false
	}, timeout, 5*time.Millisecond)
	return found
}

func testObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": testAPIVersion,
		"kind":       testKind,
		"metadata": map[string]any{
			"name":       "demo",
			"namespace":  "default",
			"uid":        "uid-123",
			"generation": int64(2),
		},
		"spec":   map[string]any{"replicas": int64(3)},
		"status": map[string]any{"healthy": true},
	}}
}

func replicaRule(script string) Rule {
	return Rule{APIVersion: testAPIVersion, Kind: testKind, Operation: "GetReplicas", CandidateScript: script}
}

func healthRule(script string) Rule {
	return Rule{APIVersion: testAPIVersion, Kind: testKind, Operation: "InterpretHealth", CandidateScript: script}
}

func reviseRule(script string, allowedFields ...string) Rule {
	return Rule{
		APIVersion:        testAPIVersion,
		Kind:              testKind,
		Operation:         "ReviseReplica",
		CandidateScript:   script,
		AllowedDiffFields: allowedFields,
	}
}

func testConfig(rules ...Rule) *Config {
	return &Config{
		Enabled:             true,
		QueueCapacity:       16,
		Workers:             1,
		ExecutionTimeout:    metav1.Duration{Duration: 200 * time.Millisecond},
		SamplingRate:        1,
		MaxInputBytes:       1 << 20,
		MaxQueuedInputBytes: 16 << 20,
		Rules:               rules,
	}
}

func startInterpreter(t *testing.T, cfg *Config) (*Interpreter, *diagnosticSink, context.CancelFunc) {
	t.Helper()
	sink := &diagnosticSink{}
	interpreter, err := NewWithDiagnose(cfg, sink.collect)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	interpreter.Start(ctx)
	t.Cleanup(cancel)
	return interpreter, sink, cancel
}

// TestGetReplicasDivergenceDetected verifies that a candidate which deliberately
// returns different replicas is flagged as Diff while the submission itself stays
// non-blocking and carries reviewable redacted summaries.
func TestGetReplicasDivergenceDetected(t *testing.T) {
	candidate := `
function GetReplicas(obj)
	return obj.spec.replicas + 1, nil
end`
	interpreter, sink, _ := startInterpreter(t, testConfig(replicaRule(candidate)))

	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")

	diagnostic := sink.waitForOutcome(t, OutcomeDiff, time.Second)
	assert.Equal(t, configv1alpha1.InterpreterOperationInterpretReplica, diagnostic.Operation)
	assert.Equal(t, testKind, diagnostic.GVK.Kind)
	assert.Contains(t, diagnostic.DiffFields, "replicas")
	assert.Equal(t, "active-v1", diagnostic.ActiveScriptVersion)
	assert.Len(t, diagnostic.CandidateScriptVersion, digestLength)
	assert.NotEmpty(t, diagnostic.InputDigest)
	assert.Greater(t, diagnostic.InputBytes, int64(0))
	assert.Equal(t, int64(2), diagnostic.ObjectGeneration)
	assert.Equal(t, "uid-123", diagnostic.ResourceUID)
}

// TestGetReplicasExactMatch verifies that an identical candidate yields Match.
func TestGetReplicasExactMatch(t *testing.T) {
	candidate := `
function GetReplicas(obj)
	return obj.spec.replicas, nil
end`
	interpreter, sink, _ := startInterpreter(t, testConfig(replicaRule(candidate)))

	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")

	diagnostic := sink.waitForOutcome(t, OutcomeMatch, time.Second)
	assert.Empty(t, diagnostic.DiffFields)
}

// TestInterpretHealthDivergenceDetected verifies that a wrong candidate health
// result is located.
func TestInterpretHealthDivergenceDetected(t *testing.T) {
	candidate := `
function InterpretHealth(obj)
	return false
end`
	interpreter, sink, _ := startInterpreter(t, testConfig(healthRule(candidate)))

	interpreter.SubmitInterpretHealth(testObject(), true, nil, "active-v1")

	diagnostic := sink.waitForOutcome(t, OutcomeDiff, time.Second)
	assert.Equal(t, configv1alpha1.InterpreterOperationInterpretHealth, diagnostic.Operation)
	assert.Equal(t, []string{"healthy"}, diagnostic.DiffFields)
}

// TestReviseReplicaIdentityViolation verifies that a candidate attempting to
// rewrite a protected identity field is always flagged as IdentityViolation, even
// when the path is listed as an allowed diff field.
func TestReviseReplicaIdentityViolation(t *testing.T) {
	candidate := `
function ReviseReplica(obj, replica)
	obj.spec.replicas = replica
	obj.metadata.name = "renamed-by-candidate"
	return obj
end`
	cfg := testConfig(reviseRule(candidate, "spec.replicas", "metadata.name"))
	interpreter, sink, _ := startInterpreter(t, cfg)

	activeRevised := testObject()
	require.NoError(t, unstructured.SetNestedField(activeRevised.Object, int64(5), "spec", "replicas"))

	interpreter.SubmitReviseReplica(testObject(), activeRevised, 5, nil, "active-v1")

	diagnostic := sink.waitForOutcome(t, OutcomeIdentityViolation, time.Second)
	assert.Contains(t, diagnostic.DiffFields, "metadata.name")
}

// TestReviseReplicaFieldBoundaries verifies that differences confined to declared
// fields are AllowedDiff, while any out-of-boundary difference becomes Diff.
func TestReviseReplicaFieldBoundaries(t *testing.T) {
	allowedCandidate := `
function ReviseReplica(obj, replica)
	obj.spec.replicas = replica + 1
	return obj
end`
	outOfBoundaryCandidate := `
function ReviseReplica(obj, replica)
	obj.spec.replicas = replica + 1
	obj.spec.image = "candidate-image"
	return obj
end`

	activeRevised := testObject()
	require.NoError(t, unstructured.SetNestedField(activeRevised.Object, int64(5), "spec", "replicas"))

	t.Run("allowed diff", func(t *testing.T) {
		interpreter, sink, _ := startInterpreter(t, testConfig(reviseRule(allowedCandidate, "spec.replicas")))
		interpreter.SubmitReviseReplica(testObject(), activeRevised, 5, nil, "active-v1")
		diagnostic := sink.waitForOutcome(t, OutcomeAllowedDiff, time.Second)
		assert.Equal(t, []string{"spec.replicas"}, diagnostic.DiffFields)
	})

	t.Run("out of boundary diff", func(t *testing.T) {
		interpreter, sink, _ := startInterpreter(t, testConfig(reviseRule(outOfBoundaryCandidate, "spec.replicas")))
		interpreter.SubmitReviseReplica(testObject(), activeRevised, 5, nil, "active-v1")
		diagnostic := sink.waitForOutcome(t, OutcomeDiff, time.Second)
		assert.Contains(t, diagnostic.DiffFields, "spec.image")
	})
}

// TestCandidateInfiniteLoopTimesOut verifies that a non-terminating candidate is
// interrupted by the execution timeout, classified as CandidateTimeout and never
// blocks the controller goroutine that submitted it.
func TestCandidateInfiniteLoopTimesOut(t *testing.T) {
	candidate := `
function GetReplicas(obj)
	while true do
	end
end`
	cfg := testConfig(replicaRule(candidate))
	cfg.ExecutionTimeout = metav1.Duration{Duration: 100 * time.Millisecond}
	interpreter, sink, _ := startInterpreter(t, cfg)

	startedAt := time.Now()
	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")
	assert.Less(t, time.Since(startedAt), 200*time.Millisecond, "Submit must stay non-blocking")

	diagnostic := sink.waitForOutcome(t, OutcomeCandidateTimeout, 2*time.Second)
	assert.Equal(t, msgCandidateTimeout, diagnostic.Message)
	assert.LessOrEqual(t, diagnostic.Duration, 2*time.Second)
}

// TestCandidateRuntimeError verifies that candidate errors are contained as
// CandidateError diagnostics and never surface on the active path. Raw error text
// (which may embed secrets) must not be retained.
func TestCandidateRuntimeError(t *testing.T) {
	const secretMarker = "SECRET_MARKER_TOKEN_123"
	candidate := fmt.Sprintf(`
function GetReplicas(obj)
	error("%s")
end`, secretMarker)
	interpreter, sink, _ := startInterpreter(t, testConfig(replicaRule(candidate)))

	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")

	diagnostic := sink.waitForOutcome(t, OutcomeCandidateError, time.Second)
	assert.Equal(t, msgCandidateFailed, diagnostic.Message)
	assert.NotContains(t, fmt.Sprintf("%+v", diagnostic), secretMarker)
}

// TestCandidateVMGlobalStateIsolation verifies that globals mutated by one candidate
// run cannot be observed by a later run, because every candidate execution gets a
// fresh isolated LState.
func TestCandidateVMGlobalStateIsolation(t *testing.T) {
	globalSetter := `
function GetReplicas(obj)
	POLLUTED_GLOBAL = "leak"
	return 3, nil
end`
	globalChecker := `
function GetReplicas(obj)
	if POLLUTED_GLOBAL ~= nil then
		error("global state leaked across shadow runs")
	end
	return 3, nil
end`
	interpreter, sink, _ := startInterpreter(t, testConfig(replicaRule(globalSetter)))

	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")
	_ = sink.waitForOutcome(t, OutcomeMatch, time.Second)

	// Replace the rule: the configuration generation bumps and the new candidate
	// asserts that no global state survived the previous isolated execution.
	require.NoError(t, interpreter.UpdateConfig(testConfig(replicaRule(globalChecker))))
	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")
	_ = sink.waitForOutcome(t, OutcomeMatch, time.Second)

	for _, diagnostic := range sink.all() {
		assert.NotEqual(t, OutcomeCandidateError, diagnostic.Outcome, "isolated VM state leaked between runs")
	}
}

// TestActiveErrorYieldsUnknown verifies that a failed active call produces an
// Unknown comparison instead of a misleading match or diff.
func TestActiveErrorYieldsUnknown(t *testing.T) {
	candidate := `
function GetReplicas(obj)
	return 3, nil
end`
	interpreter, sink, _ := startInterpreter(t, testConfig(replicaRule(candidate)))

	interpreter.SubmitGetReplicas(testObject(), 0, nil, errors.New("active boom"), "active-v1")

	diagnostic := sink.waitForOutcome(t, OutcomeUnknown, time.Second)
	assert.Equal(t, msgActiveFailed, diagnostic.Message)
	// The raw active error text is not retained.
	assert.NotContains(t, diagnostic.Message, "active boom")
}

// TestSamplingDecisions verifies that sampling is disabled at rate zero, fully
// admitted at rate one and deterministic for stable operation/UID/generation keys.
func TestSamplingDecisions(t *testing.T) {
	t.Run("rate zero skips", func(t *testing.T) {
		cfg := testConfig(replicaRule(`function GetReplicas(o) return 1, nil end`))
		cfg.SamplingRate = 0
		sink := &diagnosticSink{}
		interpreter, err := NewWithDiagnose(cfg, sink.collect)
		require.NoError(t, err)

		interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")
		diagnostic := sink.waitForOutcome(t, OutcomeSkipped, time.Second)
		assert.Equal(t, SkipReasonNotSampled, diagnostic.SkipReason)
	})

	t.Run("stable decision for same identity", func(t *testing.T) {
		cfg := testConfig(replicaRule(`function GetReplicas(o) return 3, nil end`))
		cfg.SamplingRate = 0.5
		cfg.QueueCapacity = 128
		interpreter, sink, _ := startInterpreter(t, cfg)

		// Distinct identities spread across the sampling space; each identity is
		// submitted twice so per-identity stability can be asserted.
		const identities = 40
		for round := 0; round < 2; round++ {
			for i := range identities {
				object := testObject()
				object.SetUID(types.UID(fmt.Sprintf("uid-%d", i)))
				interpreter.SubmitGetReplicas(object, 3, nil, nil, "active-v1")
			}
		}
		require.Eventually(t, func() bool { return len(sink.all()) >= 2*identities }, 3*time.Second, 5*time.Millisecond)

		// Correlate the two rounds by UID: asynchronous workers make sink ordering
		// arbitrary, but every UID must map to exactly two identical sampling decisions.
		decisions := make(map[string][]Outcome, identities)
		for _, diagnostic := range sink.all() {
			decisions[diagnostic.ResourceUID] = append(decisions[diagnostic.ResourceUID], diagnostic.Outcome)
		}
		require.Len(t, decisions, identities)
		skipped := 0
		for uid, outcomes := range decisions {
			require.Lenf(t, outcomes, 2, "identity %s must be evaluated twice", uid)
			assert.Equalf(t, outcomes[0], outcomes[1], "sampling decision for identity %s must be stable", uid)
			if outcomes[0] == OutcomeSkipped {
				skipped += 2
			}
		}
		assert.Greater(t, skipped, 0, "rate 0.5 should skip some distinct identities in practice")
		assert.Less(t, skipped, 2*identities, "rate 0.5 should admit some distinct identities in practice")
	})
}

// TestQueueFullIsExplicitAndNonBlocking verifies that a saturated bounded queue
// produces an explicit queue_full skip without blocking the submitter.
func TestQueueFullIsExplicitAndNonBlocking(t *testing.T) {
	candidate := `
function GetReplicas(obj)
	while true do
	end
end`
	cfg := testConfig(replicaRule(candidate))
	cfg.QueueCapacity = 1
	// A long timeout keeps the single worker busy throughout the submission sequence,
	// making the queue-full deterministic instead of relying on scheduling luck.
	cfg.ExecutionTimeout = metav1.Duration{Duration: 2 * time.Second}
	interpreter, sink, _ := startInterpreter(t, cfg)

	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1") // occupies the worker
	require.Eventually(t, func() bool { return len(interpreter.queue) == 0 }, time.Second, 5*time.Millisecond)
	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1") // fills the queue slot
	require.Eventually(t, func() bool { return len(interpreter.queue) == 1 }, time.Second, 5*time.Millisecond)

	startedAt := time.Now()
	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1") // must be rejected
	elapsed := time.Since(startedAt)
	assert.Less(t, elapsed, 200*time.Millisecond)

	diagnostic := sink.waitForOutcome(t, OutcomeSkipped, time.Second)
	assert.Equal(t, SkipReasonQueueFull, diagnostic.SkipReason)
}

// TestInputBudgets verifies the per-task size limit and the global queued bytes
// budget, and that reserved bytes are always released back.
func TestInputBudgets(t *testing.T) {
	t.Run("per task input too large", func(t *testing.T) {
		cfg := testConfig(replicaRule(`function GetReplicas(o) return 1, nil end`))
		cfg.MaxInputBytes = 1
		interpreter, sink, _ := startInterpreter(t, cfg)

		interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")
		diagnostic := sink.waitForOutcome(t, OutcomeSkipped, time.Second)
		assert.Equal(t, SkipReasonInputTooLarge, diagnostic.SkipReason)
	})

	t.Run("global queued bytes budget", func(t *testing.T) {
		cfg := testConfig(replicaRule(`function GetReplicas(o) while true do end end`))
		cfg.MaxQueuedInputBytes = 10
		sink := &diagnosticSink{}
		interpreter, err := NewWithDiagnose(cfg, sink.collect)
		require.NoError(t, err)
		// Without starting workers, every admitted task would remain queued, so the
		// first reservation already exceeds the tiny global budget.
		interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")
		diagnostic := sink.waitForOutcome(t, OutcomeSkipped, time.Second)
		assert.Equal(t, SkipReasonBudgetExceeded, diagnostic.SkipReason)
		assert.Zero(t, interpreter.queuedBytes.Load(), "rejected reservations must be released")
	})
}

// TestStaleGenerationResultsDiscarded verifies that configuration replacement
// invalidates queued tasks: old candidate results are never attached to the new
// script version, they are recorded as stale skips instead.
func TestStaleGenerationResultsDiscarded(t *testing.T) {
	oldCandidate := `
function GetReplicas(obj)
	while true do
	end
end`
	newCandidate := `
function GetReplicas(obj)
	return 99, nil
end`
	cfg := testConfig(replicaRule(oldCandidate))
	cfg.QueueCapacity = 2
	interpreter, sink, _ := startInterpreter(t, cfg)

	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1") // worker
	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1") // queued

	newCfg := testConfig(replicaRule(newCandidate))
	newCfg.QueueCapacity = 2
	require.NoError(t, interpreter.UpdateConfig(newCfg))
	newSnapshot := interpreter.currentSnapshot()
	newVersion := newSnapshot.rules[ruleKey{gvk: testObject().GroupVersionKind(), operation: configv1alpha1.InterpreterOperationInterpretReplica}].version

	sink.waitForCount(t, 2, 2*time.Second)
	for _, diagnostic := range sink.all() {
		assert.Equal(t, OutcomeSkipped, diagnostic.Outcome)
		assert.Equal(t, SkipReasonStale, diagnostic.SkipReason)
		assert.Equal(t, int64(1), diagnostic.ConfigGeneration)
		assert.NotEqual(t, newVersion, diagnostic.CandidateScriptVersion,
			"old-script results must never be attributed to the new script version")
	}
}

// TestCancellationDropsQueuedResults verifies that shutting down the interpreter
// discards queued work without emitting any non-skip diagnostic.
func TestCancellationDropsQueuedResults(t *testing.T) {
	candidate := `
function GetReplicas(obj)
	while true do
	end
end`
	cfg := testConfig(replicaRule(candidate))
	cfg.QueueCapacity = 4
	interpreter, sink, cancel := startInterpreter(t, cfg)

	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1") // worker
	interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1") // queued
	cancel()

	sink.waitForCount(t, 2, 2*time.Second)
	for _, diagnostic := range sink.all() {
		assert.Equal(t, OutcomeSkipped, diagnostic.Outcome)
		assert.Equal(t, SkipReasonShutdown, diagnostic.SkipReason)
	}
}

// TestRuleNotFoundAndDisabled verifies the admission skips when no rule matches or
// the feature is disabled.
func TestRuleNotFoundAndDisabled(t *testing.T) {
	t.Run("rule not found", func(t *testing.T) {
		interpreter, sink, _ := startInterpreter(t, testConfig(healthRule(`function InterpretHealth(o) return true end`)))
		interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")
		diagnostic := sink.waitForOutcome(t, OutcomeSkipped, time.Second)
		assert.Equal(t, SkipReasonRuleNotFound, diagnostic.SkipReason)
	})

	t.Run("disabled", func(t *testing.T) {
		cfg := testConfig(replicaRule(`function GetReplicas(o) return 1, nil end`))
		cfg.Enabled = false
		interpreter, sink, _ := startInterpreter(t, cfg)
		interpreter.SubmitGetReplicas(testObject(), 3, nil, nil, "active-v1")
		diagnostic := sink.waitForOutcome(t, OutcomeSkipped, time.Second)
		assert.Equal(t, SkipReasonDisabled, diagnostic.SkipReason)
	})
}

// TestDiagnosticsContainNoSourceOrValues verifies that diagnostics retain only
// structural metadata, hashes and enums: no script source and no object values.
func TestDiagnosticsContainNoSourceOrValues(t *testing.T) {
	const secretMarker = "SECRET_MARKER_IN_SCRIPT_456"
	candidate := fmt.Sprintf(`
function ReviseReplica(obj, replica)
	local token = "%s"
	obj.metadata.name = token
	return obj
end`, secretMarker)
	interpreter, sink, _ := startInterpreter(t, testConfig(reviseRule(candidate)))

	activeRevised := testObject()
	interpreter.SubmitReviseReplica(testObject(), activeRevised, 5, nil, "active-v1")
	diagnostic := sink.waitForOutcome(t, OutcomeIdentityViolation, time.Second)

	rendered := fmt.Sprintf("%+v", diagnostic)
	assert.NotContains(t, rendered, secretMarker)
	assert.NotContains(t, rendered, "renamed") // candidate-produced values must not flow into diagnostics
	assert.Len(t, diagnostic.InputDigest, digestLength)
	assert.Len(t, diagnostic.CandidateScriptVersion, digestLength)
}

func countSkipped(diagnostics []Diagnostic, reason SkipReason) int {
	count := 0
	for _, diagnostic := range diagnostics {
		if diagnostic.Outcome == OutcomeSkipped && diagnostic.SkipReason == reason {
			count++
		}
	}
	return count
}

// TestNilSafety verifies that submitting to a nil interpreter is a no-op.
func TestNilSafety(t *testing.T) {
	var interpreter *Interpreter
	assert.NotPanics(t, func() {
		interpreter.SubmitGetReplicas(testObject(), 1, nil, nil, "v")
		interpreter.SubmitInterpretHealth(testObject(), true, nil, "v")
		interpreter.SubmitReviseReplica(testObject(), testObject(), 1, nil, "v")
	})
}

// TestScriptSummaryDoesNotUseSource verifies the auto-generated script version is a
// bounded digest and never a prefix of the source.
func TestScriptSummaryDoesNotUseSource(t *testing.T) {
	script := `function GetReplicas(o) return o.spec.replicas, nil end`
	snap, err := testConfig(replicaRule(script)).compile(1)
	require.NoError(t, err)
	rule := snap.rules[ruleKey{
		gvk:       testObject().GroupVersionKind(),
		operation: configv1alpha1.InterpreterOperationInterpretReplica,
	}]
	require.NotNil(t, rule)
	assert.Len(t, rule.version, digestLength)
	assert.False(t, strings.HasPrefix(script, rule.version))
}
