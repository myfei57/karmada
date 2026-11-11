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

package declarative

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/customized/declarative/shadow"
)

const (
	testGVKAPIVersion = "example.io/v1"
	testGVKKind       = "Workload"
)

// Lua scripts backing the active interpretation path. They are shared verbatim by
// the shadow-enabled and shadow-disabled interpreters so that any output difference
// could only be introduced by the shadow machinery.
var (
	activeGetReplicasScript = `
function GetReplicas(obj)
	return obj.spec.replicas, nil
end`
	activeHealthScript = `
function InterpretHealth(obj)
	return obj.status.healthy
end`
	activeReviseScript = `
function ReviseReplica(obj, replica)
	obj.spec.replicas = replica
	return obj
end`
)

func activeCustomization() *configv1alpha1.ResourceInterpreterCustomization {
	return &configv1alpha1.ResourceInterpreterCustomization{
		ObjectMeta: metav1.ObjectMeta{Name: "test-workload"},
		Spec: configv1alpha1.ResourceInterpreterCustomizationSpec{
			Target: configv1alpha1.CustomizationTarget{
				APIVersion: testGVKAPIVersion,
				Kind:       testGVKKind,
			},
			Customizations: configv1alpha1.CustomizationRules{
				ReplicaResource: &configv1alpha1.ReplicaResourceRequirement{LuaScript: activeGetReplicasScript},
				HealthInterpretation: &configv1alpha1.HealthInterpretation{
					LuaScript: activeHealthScript,
				},
				ReplicaRevision: &configv1alpha1.ReplicaRevision{LuaScript: activeReviseScript},
			},
		},
	}
}

func workloadObject() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": testGVKAPIVersion,
		"kind":       testGVKKind,
		"metadata": map[string]any{
			"name":       "demo",
			"namespace":  "default",
			"uid":        "uid-workload-1",
			"generation": int64(1),
		},
		"spec":   map[string]any{"replicas": int64(3)},
		"status": map[string]any{"healthy": true},
	}}
}

func newLoadedInterpreter(t *testing.T, runner ShadowRunner) *ConfigurableInterpreter {
	t.Helper()
	interpreter := NewConfigurableInterpreterWithShadow(nil, runner)
	interpreter.LoadConfig([]*configv1alpha1.ResourceInterpreterCustomization{activeCustomization()})
	require.True(t, interpreter.HookEnabled(workloadObject().GroupVersionKind(), configv1alpha1.InterpreterOperationInterpretReplica))
	return interpreter
}

// countingRunner wraps a shadow runner and records the number of submissions per
// operation, proving that every active invocation produces exactly one shadow
// submission and no extra ones.
type countingRunner struct {
	inner  ShadowRunner
	mu     sync.Mutex
	counts map[string]int
}

func newCountingRunner(inner ShadowRunner) *countingRunner {
	return &countingRunner{inner: inner, counts: map[string]int{}}
}

// Compile-time check that the counting runner satisfies the ShadowRunner contract.
var _ ShadowRunner = (*countingRunner)(nil)

func (r *countingRunner) SubmitGetReplicas(object *unstructured.Unstructured, activeReplica int32, activeRequires *workv1alpha2.ReplicaRequirements, activeErr error, activeScriptVersion string) {
	r.mark("GetReplicas")
	r.inner.SubmitGetReplicas(object, activeReplica, activeRequires, activeErr, activeScriptVersion)
}

func (r *countingRunner) SubmitInterpretHealth(object *unstructured.Unstructured, healthy bool, activeErr error, activeScriptVersion string) {
	r.mark("InterpretHealth")
	r.inner.SubmitInterpretHealth(object, healthy, activeErr, activeScriptVersion)
}

func (r *countingRunner) SubmitReviseReplica(object, revised *unstructured.Unstructured, desiredReplica int64, activeErr error, activeScriptVersion string) {
	r.mark("ReviseReplica")
	r.inner.SubmitReviseReplica(object, revised, desiredReplica, activeErr, activeScriptVersion)
}

func (r *countingRunner) mark(operation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[operation]++
}

func (r *countingRunner) snapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make(map[string]int, len(r.counts))
	for key, value := range r.counts {
		copied[key] = value
	}
	return copied
}

// TestActivePathInvariantUnderShadow drives identical active calls through a plain
// interpreter and a shadow-enabled interpreter and asserts byte-identical results.
// Candidate scripts deliberately diverge (wrong replicas, wrong health, renamed
// object) but none of that can feed back into the active outputs.
func TestActivePathInvariantUnderShadow(t *testing.T) {
	shadowCfg := &shadow.Config{
		Enabled:             true,
		QueueCapacity:       32,
		Workers:             2,
		ExecutionTimeout:    metav1.Duration{Duration: 200 * time.Millisecond},
		SamplingRate:        1,
		MaxInputBytes:       1 << 20,
		MaxQueuedInputBytes: 16 << 20,
		Rules: []shadow.Rule{
			{
				APIVersion: testGVKAPIVersion,
				Kind:       testGVKKind,
				Operation:  "GetReplicas",
				CandidateScript: `
function GetReplicas(obj)
	return obj.spec.replicas + 100, nil
end`,
			},
			{
				APIVersion:      testGVKAPIVersion,
				Kind:            testGVKKind,
				Operation:       "InterpretHealth",
				CandidateScript: `function InterpretHealth(obj) return false end`,
			},
			{
				APIVersion: testGVKAPIVersion,
				Kind:       testGVKKind,
				Operation:  "ReviseReplica",
				CandidateScript: `
function ReviseReplica(obj, replica)
	obj.spec.replicas = replica
	obj.metadata.name = "renamed-by-candidate"
	return obj
end`,
				AllowedDiffFields: []string{"spec.replicas"},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := newShadowSink()
	shadowInterpreter, err := shadow.NewWithDiagnose(shadowCfg, sink.collect)
	require.NoError(t, err)
	shadowInterpreter.Start(ctx)
	runner := newCountingRunner(shadowInterpreter)

	plain := newLoadedInterpreter(t, nil)
	shadowed := newLoadedInterpreter(t, runner)

	const rounds = 5
	for range rounds {
		plainObject := workloadObject()
		shadowObject := workloadObject()

		plainReplica, plainRequires, plainEnabled, plainErr := plain.GetReplicas(plainObject)
		shadowReplica, shadowRequires, shadowEnabled, shadowErr := shadowed.GetReplicas(shadowObject)
		assert.Equal(t, plainErr, shadowErr)
		assert.Equal(t, plainEnabled, shadowEnabled)
		assert.True(t, shadowEnabled)
		assert.Equal(t, plainReplica, shadowReplica)
		assert.Equal(t, plainRequires, shadowRequires)
		assert.Equal(t, int32(3), shadowReplica, "candidate divergence must not change the active replica")

		plainHealthy, plainHealthEnabled, plainHealthErr := plain.InterpretHealth(plainObject)
		shadowHealthy, shadowHealthEnabled, shadowHealthErr := shadowed.InterpretHealth(shadowObject)
		assert.Equal(t, plainHealthErr, shadowHealthErr)
		assert.Equal(t, plainHealthEnabled, shadowHealthEnabled)
		assert.True(t, shadowHealthEnabled)
		assert.Equal(t, plainHealthy, shadowHealthy)
		assert.True(t, shadowHealthy, "candidate health divergence must not change the active health")

		plainRevised, plainReviseEnabled, plainReviseErr := plain.ReviseReplica(plainObject, 7)
		shadowRevised, shadowReviseEnabled, shadowReviseErr := shadowed.ReviseReplica(shadowObject, 7)
		require.NoError(t, plainReviseErr)
		require.NoError(t, shadowReviseErr)
		assert.True(t, plainReviseEnabled)
		assert.True(t, shadowReviseEnabled)
		plainJSON, err := plainRevised.MarshalJSON()
		require.NoError(t, err)
		shadowJSON, err := shadowRevised.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEq(t, string(plainJSON), string(shadowJSON))
		assert.Equal(t, "demo", shadowRevised.GetName(), "candidate identity change must not change the active revised object")

		// The caller-owned input object must remain untouched by candidate execution.
		assert.Equal(t, "demo", shadowObject.GetName())
		assert.Equal(t, int64(3), nestedInt64(t, shadowObject, "spec", "replicas"))
	}

	// One shadow submission per active invocation, no more, no less.
	counts := runner.snapshot()
	assert.Equal(t, rounds, counts["GetReplicas"])
	assert.Equal(t, rounds, counts["InterpretHealth"])
	assert.Equal(t, rounds, counts["ReviseReplica"])

	// The shadow side must independently locate every injected divergence.
	sink.waitForOutcome(t, shadow.OutcomeDiff, configv1alpha1.InterpreterOperationInterpretReplica)
	sink.waitForOutcome(t, shadow.OutcomeDiff, configv1alpha1.InterpreterOperationInterpretHealth)
	sink.waitForOutcome(t, shadow.OutcomeIdentityViolation, configv1alpha1.InterpreterOperationReviseReplica)
}

// TestCandidateInfiniteLoopDoesNotDisturbActive verifies that a hanging candidate is
// contained by the execution timeout while active calls keep succeeding immediately
// with their real results.
func TestCandidateInfiniteLoopDoesNotDisturbActive(t *testing.T) {
	shadowCfg := &shadow.Config{
		Enabled:             true,
		QueueCapacity:       8,
		Workers:             1,
		ExecutionTimeout:    metav1.Duration{Duration: 100 * time.Millisecond},
		SamplingRate:        1,
		MaxInputBytes:       1 << 20,
		MaxQueuedInputBytes: 16 << 20,
		Rules: []shadow.Rule{{
			APIVersion:      testGVKAPIVersion,
			Kind:            testGVKKind,
			Operation:       "GetReplicas",
			CandidateScript: `function GetReplicas(obj) while true do end end`,
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := newShadowSink()
	shadowInterpreter, err := shadow.NewWithDiagnose(shadowCfg, sink.collect)
	require.NoError(t, err)
	shadowInterpreter.Start(ctx)

	interpreter := newLoadedInterpreter(t, shadowInterpreter)

	for range 3 {
		startedAt := time.Now()
		replica, requires, enabled, callErr := interpreter.GetReplicas(workloadObject())
		assert.Less(t, time.Since(startedAt), 300*time.Millisecond, "active call must not wait for the hanging candidate")
		require.NoError(t, callErr)
		assert.True(t, enabled)
		assert.Equal(t, int32(3), replica)
		assert.Nil(t, requires)
	}

	sink.waitForOutcome(t, shadow.OutcomeCandidateTimeout, configv1alpha1.InterpreterOperationInterpretReplica)
}

// shadowSink is a small adapter reusing the diagnostic collection pattern inside the
// declarative package without importing test-only helpers from the shadow package.
type shadowSink struct {
	mu    sync.Mutex
	items []shadow.Diagnostic
}

func newShadowSink() *shadowSink {
	return &shadowSink{}
}

func (s *shadowSink) collect(diagnostic shadow.Diagnostic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, diagnostic)
}

func (s *shadowSink) waitForOutcome(t *testing.T, outcome shadow.Outcome, operation configv1alpha1.InterpreterOperation) shadow.Diagnostic {
	t.Helper()
	var found shadow.Diagnostic
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, diagnostic := range s.items {
			if diagnostic.Outcome == outcome && diagnostic.Operation == operation {
				found = diagnostic
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond)
	return found
}

func nestedInt64(t *testing.T, object *unstructured.Unstructured, fields ...string) int64 {
	t.Helper()
	value, found, err := unstructured.NestedFieldNoCopy(object.Object, fields...)
	require.NoError(t, err)
	require.True(t, found)
	number, ok := value.(int64)
	require.True(t, ok)
	return number
}
