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
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync/atomic"
	"time"

	lua "github.com/yuin/gopher-lua"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/customized/declarative/luavm"
)

// Interpreter runs candidate Lua customization scripts alongside the active
// interpretation path. It is strictly observational: submissions are non-blocking,
// candidate results are never returned to controllers and cannot influence Work,
// replica or status decisions. Only explicitly allowed local Lua operations
// (GetReplicas, InterpretHealth, ReviseReplica) participate; webhook and external
// interpreters are not wired in.
type Interpreter struct {
	// vm is dedicated to shadow evaluations and is never shared with the active
	// interpretation path. Candidate executions additionally use isolated LStates,
	// so VM globals cannot leak between runs either.
	vm *luavm.VM

	// queue is a fixed-capacity channel. A saturated queue produces an explicit
	// Skipped(queue_full) record instead of blocking the calling controller.
	queue chan *frozenTask

	// snapshot holds the current *snapshot. Every replacement bumps generation.
	snapshot atomic.Value
	// generation tracks the snapshot version stored in snapshot.
	generation atomic.Int64
	// queuedBytes tracks the total serialized input bytes of queued and in-flight
	// tasks, enforcing the global input bytes budget without holding a lock.
	queuedBytes atomic.Int64

	// started guarantees the fixed worker pool is started at most once.
	started atomic.Bool

	// diagnose receives every produced diagnostic. It is overridable in tests.
	diagnose func(Diagnostic)
}

// New builds a shadow interpreter from the given configuration. A nil or disabled
// configuration yields an interpreter that rejects every submission with a bounded
// skip record, allowing callers to wire the feature unconditionally. User-provided
// configurations should be defaulted via SetDefaults (LoadConfigFromFile already
// does this); programmatic callers may set every field explicitly, including an
// explicit zero SamplingRate to disable sampling.
func New(cfg *Config) (*Interpreter, error) {
	return NewWithDiagnose(cfg, defaultDiagnose)
}

// NewWithDiagnose behaves like New but routes every diagnostic to the supplied sink
// instead of the default logging sink. It is intended for tests and for environments
// that collect shadow diagnostics through their own observability pipeline.
func NewWithDiagnose(cfg *Config, diagnose func(Diagnostic)) (*Interpreter, error) {
	if diagnose == nil {
		diagnose = defaultDiagnose
	}
	interpreter := &Interpreter{
		// The pool itself is never used: isolated LStates are created per task and
		// closed afterwards. A pool size of one keeps the embedded VM lightweight.
		vm:       luavm.New(false, 1),
		diagnose: diagnose,
	}

	initial, err := cfg.compile(1)
	if err != nil {
		return nil, err
	}
	interpreter.snapshot.Store(initial)
	interpreter.generation.Store(1)
	interpreter.queue = make(chan *frozenTask, initial.config.QueueCapacity)
	return interpreter, nil
}

// Start launches the fixed worker pool. It returns immediately and workers exit
// when ctx is canceled. Starting more than once is a no-op.
func (s *Interpreter) Start(ctx context.Context) {
	if s == nil || !s.started.CompareAndSwap(false, true) {
		return
	}
	workers := s.currentSnapshot().config.Workers
	for range workers {
		go s.worker(ctx)
	}
	// On shutdown, drain tasks that never reached a worker so that every admitted
	// task receives a terminal diagnostic and its reserved byte budget is released.
	// The queue is fixed-capacity, so draining is always bounded and never executes
	// any candidate script.
	go func() {
		<-ctx.Done()
		for {
			select {
			case task := <-s.queue:
				s.queuedBytes.Add(-task.inputBytes)
				s.recordSkip(task, SkipReasonShutdown)
			default:
				return
			}
		}
	}()
	klog.InfoS("Shadow resource interpreter started", "workers", workers,
		"queueCapacity", s.currentSnapshot().config.QueueCapacity)
}

// UpdateConfig atomically replaces the candidate rules and runtime limits. The
// generation is bumped, so tasks already queued against the previous generation
// are discarded by workers and their results are never attached to the new version.
// Queue capacity and worker count are fixed at construction and intentionally not
// changed here; sampling, budgets, timeouts and rules take effect immediately.
func (s *Interpreter) UpdateConfig(cfg *Config) error {
	if s == nil {
		return errors.New("shadow interpreter is not initialized")
	}
	nextGeneration := s.generation.Load() + 1
	updated, err := cfg.compile(nextGeneration)
	if err != nil {
		return err
	}
	s.snapshot.Store(updated)
	s.generation.Store(nextGeneration)
	klog.InfoS("Shadow resource interpreter configuration replaced", "generation", nextGeneration,
		"rules", len(updated.rules))
	return nil
}

// SubmitGetReplicas enqueues a shadow evaluation for InterpretReplica. It never
// blocks and never returns an error: every failure mode becomes a bounded
// diagnostic. The active replica result and error are passed by value semantics.
func (s *Interpreter) SubmitGetReplicas(object *unstructured.Unstructured, activeReplica int32, activeRequires *workv1alpha2.ReplicaRequirements, activeErr error, activeScriptVersion string) {
	if s == nil || object == nil {
		return
	}

	var requiresJSON []byte
	if activeRequires != nil {
		requiresJSON, _ = json.Marshal(activeRequires)
	}

	task := &frozenTask{
		gvk:                 object.GroupVersionKind(),
		operation:           configv1alpha1.InterpreterOperationInterpretReplica,
		activeOK:            activeErr == nil,
		activeReplica:       activeReplica,
		activeResult:        requiresJSON,
		activeScriptVersion: activeScriptVersion,
	}
	s.submit(object, task)
}

// SubmitInterpretHealth enqueues a shadow evaluation for InterpretHealth.
func (s *Interpreter) SubmitInterpretHealth(object *unstructured.Unstructured, healthy bool, activeErr error, activeScriptVersion string) {
	if s == nil || object == nil {
		return
	}

	task := &frozenTask{
		gvk:                 object.GroupVersionKind(),
		operation:           configv1alpha1.InterpreterOperationInterpretHealth,
		activeOK:            activeErr == nil,
		activeHealthy:       healthy,
		activeScriptVersion: activeScriptVersion,
	}
	s.submit(object, task)
}

// SubmitReviseReplica enqueues a shadow evaluation for ReviseReplica. The active
// revised object is frozen by value; a nil revised object is accepted together with
// an active error and yields an Unknown comparison.
func (s *Interpreter) SubmitReviseReplica(object, revised *unstructured.Unstructured, desiredReplica int64, activeErr error, activeScriptVersion string) {
	if s == nil || object == nil {
		return
	}

	var activeResult []byte
	if revised != nil {
		activeResult, _ = revised.MarshalJSON()
	}

	task := &frozenTask{
		gvk:                 object.GroupVersionKind(),
		operation:           configv1alpha1.InterpreterOperationReviseReplica,
		activeOK:            activeErr == nil,
		activeResult:        activeResult,
		activeScriptVersion: activeScriptVersion,
		reviseReplica:       desiredReplica,
	}
	s.submit(object, task)
}

// submit runs all admission gates and performs a non-blocking enqueue. Any panic is
// contained here so that shadow bookkeeping can never disrupt the active path.
func (s *Interpreter) submit(object *unstructured.Unstructured, task *frozenTask) {
	defer func() {
		if recovered := recover(); recovered != nil {
			klog.ErrorS(fmt.Errorf("%v", recovered), "Panic while submitting shadow task, ignoring")
		}
	}()

	snap := s.currentSnapshot()
	task.resourceUID = string(object.GetUID())
	task.resourceName = object.GetName()
	task.objectGeneration = object.GetGeneration()

	if !snap.config.Enabled {
		s.recordSkip(task, SkipReasonDisabled)
		return
	}

	rule, exist := snap.rules[ruleKey{gvk: task.gvk, operation: task.operation}]
	if !exist {
		s.recordSkip(task, SkipReasonRuleNotFound)
		return
	}

	if !isSampled(snap.config.SamplingRate, task) {
		s.recordSkip(task, SkipReasonNotSampled)
		return
	}

	inputJSON, err := object.MarshalJSON()
	if err != nil {
		// A marshalling failure reflects an object the active path could not freeze;
		// treat it as an over-budget style skip without touching the active result.
		s.recordSkip(task, SkipReasonInputTooLarge)
		return
	}
	task.inputObjects = [][]byte{inputJSON}
	task.inputBytes = int64(len(inputJSON))
	task.inputDigest = digestBytes(inputJSON)

	if task.inputBytes > int64(snap.config.MaxInputBytes) {
		s.recordSkip(task, SkipReasonInputTooLarge)
		return
	}

	// Reserve the global bytes budget before enqueue and release it after processing.
	if s.queuedBytes.Add(task.inputBytes) > snap.config.MaxQueuedInputBytes {
		s.queuedBytes.Add(-task.inputBytes)
		s.recordSkip(task, SkipReasonBudgetExceeded)
		return
	}

	task.rule = rule
	task.configGeneration = snap.generation

	select {
	case s.queue <- task:
	default:
		// The queue is bounded: never block the controller goroutine or controller locks.
		s.queuedBytes.Add(-task.inputBytes)
		s.recordSkip(task, SkipReasonQueueFull)
	}
}

// worker consumes tasks until the context is canceled. The pool size is fixed, so
// no per-task goroutine is ever created.
func (s *Interpreter) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.queue:
			s.process(ctx, task)
		}
	}
}

// process evaluates one task and releases its byte budget. All failure modes become
// diagnostics; the method never returns an error to the caller.
func (s *Interpreter) process(parent context.Context, task *frozenTask) {
	defer s.queuedBytes.Add(-task.inputBytes)
	defer func() {
		if recovered := recover(); recovered != nil {
			diagnostic := s.baseDiagnostic(task)
			diagnostic.Outcome = OutcomeCandidateError
			// The panic value may embed script-derived text, so only a static
			// category is retained to avoid leaking source or secret state.
			diagnostic.Message = msgCandidatePanic
			s.emit(diagnostic)
		}
	}()

	if parent.Err() != nil {
		// On shutdown, queued results are dropped rather than attached to any version.
		s.recordSkip(task, SkipReasonShutdown)
		return
	}
	// A configuration generation change invalidates the captured candidate script.
	if s.generation.Load() != task.configGeneration {
		s.recordSkip(task, SkipReasonStale)
		return
	}

	timeout := s.currentSnapshot().config.ExecutionTimeout.Duration
	execCtx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	startedAt := time.Now()
	diagnostic := s.evaluate(execCtx, parent, task)
	diagnostic.Duration = time.Since(startedAt)

	// Re-check after evaluation: a replacement that happened while the candidate was
	// running must not surface an old-script result under the new generation.
	if s.generation.Load() != task.configGeneration {
		s.recordSkip(task, SkipReasonStale)
		return
	}
	if parent.Err() != nil {
		s.recordSkip(task, SkipReasonShutdown)
		return
	}

	s.emit(diagnostic)
}

// evaluate runs the candidate script on a frozen input and classifies the result.
func (s *Interpreter) evaluate(execCtx, parent context.Context, task *frozenTask) Diagnostic {
	diagnostic := s.baseDiagnostic(task)

	inputObject, err := decodeInput(task.inputObjects[0])
	if err != nil {
		diagnostic.Outcome = OutcomeCandidateError
		diagnostic.Message = msgInputDecode
		return diagnostic
	}

	switch task.operation {
	case configv1alpha1.InterpreterOperationInterpretReplica:
		s.evaluateGetReplicas(execCtx, task, inputObject, &diagnostic)
	case configv1alpha1.InterpreterOperationInterpretHealth:
		s.evaluateHealth(execCtx, task, inputObject, &diagnostic)
	case configv1alpha1.InterpreterOperationReviseReplica:
		s.evaluateReviseReplica(execCtx, task, inputObject, &diagnostic)
	default:
		// Unreachable: admission only allows the closed operation set.
		diagnostic.Outcome = OutcomeSkipped
		diagnostic.SkipReason = SkipReasonRuleNotFound
	}

	// Parent cancellation overrides every other outcome: nothing is reported on shutdown.
	if parent.Err() != nil {
		diagnostic.Outcome = OutcomeSkipped
		diagnostic.SkipReason = SkipReasonShutdown
	}
	return diagnostic
}

// evaluateGetReplicas evaluates a candidate GetReplicas script.
func (s *Interpreter) evaluateGetReplicas(ctx context.Context, task *frozenTask, inputObject *unstructured.Unstructured, diagnostic *Diagnostic) {
	results, err := s.vm.RunScriptIsolated(ctx, task.rule.script, "GetReplicas", 2, inputObject)
	if err != nil {
		s.classifyCandidateError(ctx, err, diagnostic)
		return
	}

	candidateReplica, err := luavm.ConvertLuaResultToInt(results[0])
	if err != nil {
		diagnostic.Outcome = OutcomeCandidateError
		diagnostic.Message = msgUnexpectedResult
		return
	}

	candidateRequires := &workv1alpha2.ReplicaRequirements{}
	switch results[1].Type() {
	case lua.LTTable:
		if err := luavm.ConvertLuaResultInto(results[1].(*lua.LTable), candidateRequires); err != nil {
			diagnostic.Outcome = OutcomeCandidateError
			diagnostic.Message = msgUnexpectedResult
			return
		}
	case lua.LTNil:
		candidateRequires = nil
	default:
		diagnostic.Outcome = OutcomeCandidateError
		diagnostic.Message = msgUnexpectedResult
		return
	}

	if !task.activeOK {
		diagnostic.Outcome = OutcomeUnknown
		diagnostic.Message = msgActiveFailed
		return
	}

	activeRequires := decodeReplicaRequirements(task.activeResult)
	comparison := compareReplicas(task.activeReplica, activeRequires, candidateRequires, candidateReplica)
	diagnostic.Outcome = comparison.outcome
	diagnostic.DiffFields = comparison.diffFields
}

// evaluateHealth evaluates a candidate InterpretHealth script.
func (s *Interpreter) evaluateHealth(ctx context.Context, task *frozenTask, inputObject *unstructured.Unstructured, diagnostic *Diagnostic) {
	results, err := s.vm.RunScriptIsolated(ctx, task.rule.script, "InterpretHealth", 1, inputObject)
	if err != nil {
		s.classifyCandidateError(ctx, err, diagnostic)
		return
	}

	candidateHealthy, err := luavm.ConvertLuaResultToBool(results[0])
	if err != nil {
		diagnostic.Outcome = OutcomeCandidateError
		diagnostic.Message = msgUnexpectedResult
		return
	}

	if !task.activeOK {
		diagnostic.Outcome = OutcomeUnknown
		diagnostic.Message = msgActiveFailed
		return
	}

	diagnostic.Outcome, diagnostic.DiffFields = compareHealth(task.activeHealthy, candidateHealthy)
}

// evaluateReviseReplica evaluates a candidate ReviseReplica script and enforces the
// declared field boundaries and protected identity fields.
func (s *Interpreter) evaluateReviseReplica(ctx context.Context, task *frozenTask, inputObject *unstructured.Unstructured, diagnostic *Diagnostic) {
	results, err := s.vm.RunScriptIsolated(ctx, task.rule.script, "ReviseReplica", 1, inputObject, task.reviseReplica)
	if err != nil {
		s.classifyCandidateError(ctx, err, diagnostic)
		return
	}

	if results[0].Type() != lua.LTTable {
		diagnostic.Outcome = OutcomeCandidateError
		diagnostic.Message = msgUnexpectedResult
		return
	}

	candidateObject := &unstructured.Unstructured{}
	if err := luavm.ConvertLuaResultInto(results[0].(*lua.LTable), candidateObject, inputObject); err != nil {
		diagnostic.Outcome = OutcomeCandidateError
		diagnostic.Message = msgUnexpectedResult
		return
	}
	candidateJSON, err := candidateObject.MarshalJSON()
	if err != nil {
		diagnostic.Outcome = OutcomeCandidateError
		diagnostic.Message = msgUnexpectedResult
		return
	}

	if !task.activeOK {
		diagnostic.Outcome = OutcomeUnknown
		diagnostic.Message = msgActiveFailed
		return
	}

	comparison := compareRevisedObject(task.activeResult, candidateJSON, task.rule.allowedFields)
	diagnostic.Outcome = comparison.outcome
	diagnostic.DiffFields = comparison.diffFields
}

// classifyCandidateError distinguishes execution timeouts from ordinary candidate
// script errors so that the two are observable separately. Only static category
// messages are retained because raw error strings may embed script-derived content.
func (s *Interpreter) classifyCandidateError(ctx context.Context, err error, diagnostic *Diagnostic) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		diagnostic.Outcome = OutcomeCandidateTimeout
		diagnostic.Message = msgCandidateTimeout
		return
	}
	diagnostic.Outcome = OutcomeCandidateError
	diagnostic.Message = msgCandidateFailed
}

// recordSkip emits a bounded skip diagnostic and the corresponding metric.
func (s *Interpreter) recordSkip(task *frozenTask, reason SkipReason) {
	diagnostic := s.baseDiagnostic(task)
	diagnostic.Outcome = OutcomeSkipped
	diagnostic.SkipReason = reason
	s.emit(diagnostic)
}

// baseDiagnostic populates the redacted, reviewable fields shared by every outcome.
func (s *Interpreter) baseDiagnostic(task *frozenTask) Diagnostic {
	return Diagnostic{
		Operation:              task.operation,
		GVK:                    task.gvk,
		ResourceUID:            task.resourceUID,
		ResourceName:           task.resourceName,
		ObjectGeneration:       task.objectGeneration,
		ConfigGeneration:       task.configGeneration,
		ActiveScriptVersion:    task.activeScriptVersion,
		CandidateScriptVersion: task.ruleVersion(),
		InputBytes:             task.inputBytes,
		InputDigest:            task.inputDigest,
	}
}

// emit sends a diagnostic to the configured sink and records metrics.
func (s *Interpreter) emit(diagnostic Diagnostic) {
	RecordShadowEvaluation(diagnostic.Operation, diagnostic.GVK.Group, diagnostic.GVK.Kind, diagnostic.Outcome)
	if diagnostic.Outcome != OutcomeSkipped {
		RecordShadowDuration(diagnostic.Operation, diagnostic.GVK.Group, diagnostic.GVK.Kind, diagnostic.Duration.Seconds())
	}
	s.diagnose(diagnostic)
}

// currentSnapshot returns the active configuration snapshot.
func (s *Interpreter) currentSnapshot() *snapshot {
	return s.snapshot.Load().(*snapshot)
}

// ruleVersion safely extracts the candidate script version captured by the task.
func (t *frozenTask) ruleVersion() string {
	if t.rule == nil {
		return ""
	}
	return t.rule.version
}

// isSampled makes a deterministic sampling decision from operation, resource UID
// and object generation. The same input therefore maps to the same decision while
// the configuration generation is unchanged.
func isSampled(rate float64, task *frozenTask) bool {
	if rate >= 1 {
		return true
	}
	if rate <= 0 {
		return false
	}

	hasher := fnv.New64a()
	// UID is the stable identity; namespace/name is only a fallback for test objects
	// that are created without a UID, so the key remains well-defined.
	identity := task.resourceUID
	if identity == "" {
		identity = task.gvk.String() + "/" + task.resourceName
	}
	_, _ = fmt.Fprintf(hasher, "%s|%s|%d", task.operation, identity, task.objectGeneration)

	threshold := uint64(rate * float64(samplingDenominator))
	return hasher.Sum64()%samplingDenominator < threshold
}

// decodeInput rebuilds an independent unstructured object from frozen JSON bytes.
func decodeInput(data []byte) (*unstructured.Unstructured, error) {
	object := &unstructured.Unstructured{}
	if err := object.UnmarshalJSON(data); err != nil {
		return nil, err
	}
	return object, nil
}

// decodeReplicaRequirements restores the active replica requirements snapshot.
func decodeReplicaRequirements(data []byte) *workv1alpha2.ReplicaRequirements {
	if len(data) == 0 {
		return nil
	}
	requirements := &workv1alpha2.ReplicaRequirements{}
	if err := json.Unmarshal(data, requirements); err != nil {
		return nil
	}
	return requirements
}

// defaultDiagnose logs structured, redacted diagnostics. Only structural paths,
// hashes, sizes and enum labels are logged; object values and script source are not.
func defaultDiagnose(diagnostic Diagnostic) {
	arguments := []any{
		"operation", diagnostic.Operation,
		"group", diagnostic.GVK.Group,
		"kind", diagnostic.GVK.Kind,
		"outcome", diagnostic.Outcome,
		"resourceUID", diagnostic.ResourceUID,
		"resourceName", diagnostic.ResourceName,
		"objectGeneration", diagnostic.ObjectGeneration,
		"configGeneration", diagnostic.ConfigGeneration,
		"activeScriptVersion", diagnostic.ActiveScriptVersion,
		"candidateScriptVersion", diagnostic.CandidateScriptVersion,
		"inputBytes", diagnostic.InputBytes,
		"inputDigest", diagnostic.InputDigest,
	}
	if diagnostic.SkipReason != "" {
		arguments = append(arguments, "skipReason", diagnostic.SkipReason)
	}
	if len(diagnostic.DiffFields) > 0 {
		arguments = append(arguments, "diffFields", diagnostic.DiffFields)
	}
	if diagnostic.Message != "" {
		arguments = append(arguments, "message", diagnostic.Message)
	}

	if diagnostic.Outcome == OutcomeMatch || diagnostic.Outcome == OutcomeAllowedDiff || diagnostic.Outcome == OutcomeSkipped {
		klog.V(3).InfoS("Shadow resource interpreter evaluation", arguments...)
		return
	}
	klog.InfoS("Shadow resource interpreter divergence detected", arguments...)
}
