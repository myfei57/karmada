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
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
)

// Outcome describes how the candidate script result relates to the active result.
// All values are fixed enums so that they can be used as bounded metric labels.
type Outcome string

const (
	// OutcomeMatch means the candidate result is exactly equal to the active result.
	OutcomeMatch Outcome = "Match"
	// OutcomeDiff means the candidate result differs from the active result and the
	// difference is not covered by the declared field boundaries.
	OutcomeDiff Outcome = "Diff"
	// OutcomeAllowedDiff means the only differences are on fields explicitly declared
	// as tolerated by the shadow rule.
	OutcomeAllowedDiff Outcome = "AllowedDiff"
	// OutcomeIdentityViolation means the candidate attempted to change a protected
	// identity field (e.g. apiVersion, kind or metadata.name). Such a change is never
	// tolerated regardless of the declared field boundaries.
	OutcomeIdentityViolation Outcome = "IdentityViolation"
	// OutcomeCandidateError means the candidate script failed at runtime.
	OutcomeCandidateError Outcome = "CandidateError"
	// OutcomeCandidateTimeout means the candidate script did not finish within the
	// configured execution timeout.
	OutcomeCandidateTimeout Outcome = "CandidateTimeout"
	// OutcomeUnknown means the active interpretation failed, so there is no trustworthy
	// baseline to compare the candidate result against.
	OutcomeUnknown Outcome = "Unknown"
	// OutcomeSkipped means the shadow execution was not run for a bounded reason
	// (disabled, not sampled, over budget, queue full, stale configuration and so on).
	OutcomeSkipped Outcome = "Skipped"
)

// SkipReason explains why a shadow task was not executed. Values form a fixed enum
// that is safe to use as a metric label.
type SkipReason string

const (
	// SkipReasonDisabled means shadow interpretation is not enabled.
	SkipReasonDisabled SkipReason = "disabled"
	// SkipReasonRuleNotFound means no candidate rule is declared for the GVK/operation.
	SkipReasonRuleNotFound SkipReason = "rule_not_found"
	// SkipReasonNotSampled means the stable sampling decision did not select this input.
	SkipReasonNotSampled SkipReason = "not_sampled"
	// SkipReasonInputTooLarge means the serialized input exceeds the per-task byte budget.
	SkipReasonInputTooLarge SkipReason = "input_too_large"
	// SkipReasonBudgetExceeded means admitting the task would exceed the total queued
	// input bytes budget.
	SkipReasonBudgetExceeded SkipReason = "budget_exceeded"
	// SkipReasonQueueFull means the bounded task queue is saturated.
	SkipReasonQueueFull SkipReason = "queue_full"
	// SkipReasonStale means the configuration generation changed before the task was
	// processed, so the result must not be attached to the new script version.
	SkipReasonStale SkipReason = "stale"
	// SkipReasonShutdown means the interpreter was shutting down when the task was due.
	SkipReasonShutdown SkipReason = "shutdown"
)

// maxReportedDiffFields bounds the number of field paths carried by a single
// diagnostic so that a pathological object cannot produce unbounded payloads.
const maxReportedDiffFields = 20

// Diagnostic messages are static category descriptors rather than script-derived
// text. Candidate scripts can raise arbitrary strings (including secret material),
// so raw error messages are never retained: keeping fixed categories guarantees
// that neither script source nor secret state can leak through diagnostics.
const (
	msgActiveFailed     = "active interpretation failed"
	msgCandidateFailed  = "candidate script execution failed"
	msgCandidateTimeout = "candidate script execution timed out"
	msgCandidatePanic   = "candidate script panicked"
	msgUnexpectedResult = "candidate script returned an unexpected result type"
	msgInputDecode      = "failed to decode frozen shadow input"
)

// Diagnostic is the only externally visible effect of a shadow evaluation.
// It never contains object values, script source or secret material: only
// structural field paths, hashes, sizes and enum labels are retained so that
// the record can be reviewed without leaking sensitive state.
type Diagnostic struct {
	// Operation is the interpreted operation.
	Operation configv1alpha1.InterpreterOperation
	// GVK identifies the resource type. It is used for metric labels, not resource
	// names or UIDs, keeping the label cardinality bounded.
	GVK schema.GroupVersionKind
	// Outcome is the classification of the evaluation.
	Outcome Outcome
	// SkipReason is populated only when Outcome is OutcomeSkipped.
	SkipReason SkipReason
	// ResourceUID and ResourceName identify the concrete object in logs only. They
	// must never be promoted to metric labels.
	ResourceUID  string
	ResourceName string
	// ObjectGeneration is the metadata.generation of the sampled object.
	ObjectGeneration int64
	// ConfigGeneration is the shadow configuration generation captured at submit
	// time. A processed task whose generation is no longer current is discarded.
	ConfigGeneration int64
	// ActiveScriptVersion and CandidateScriptVersion are content hashes (or explicit
	// opaque versions) of the two scripts. The raw script source is never retained.
	ActiveScriptVersion    string
	CandidateScriptVersion string
	// InputBytes is the serialized size of the frozen input deep copy.
	InputBytes int64
	// InputDigest is a short hash of the frozen input, allowing reviewers to correlate
	// re-evaluations of the same input without retaining the input itself.
	InputDigest string
	// DiffFields lists the structural JSON paths whose values differ, capped at
	// maxReportedDiffFields. Identity violations are always reported first.
	DiffFields []string
	// Message carries a short, redacted error description for candidate failures.
	Message string
	// Duration is the candidate execution time.
	Duration time.Duration
}

// frozenTask is a self-contained shadow work item. All inputs are materialized as
// immutable serialized snapshots at submit time, so the candidate can never mutate
// an object observed by the active interpretation path, and a late worker still
// evaluates exactly the input that the active call saw.
type frozenTask struct {
	gvk       schema.GroupVersionKind
	operation configv1alpha1.InterpreterOperation

	resourceUID      string
	resourceName     string
	objectGeneration int64

	// inputObjects holds the frozen JSON encoding of every input unstructured object
	// (one for GetReplicas/InterpretHealth, two for ReviseReplica-style flows).
	inputObjects [][]byte
	inputBytes   int64
	inputDigest  string
	// reviseReplica is the desired replica argument of a ReviseReplica operation.
	reviseReplica int64

	// activeOK is false when the active call returned an error, in which case the
	// comparison yields OutcomeUnknown because there is no trustworthy baseline.
	activeOK bool
	// activeResult holds the JSON-encoded active result for object-returning
	// operations, or the primitive encoding for scalar operations.
	activeResult []byte
	// activeReplica / activeHealthy carry the scalar active results without
	// requiring another decoding round.
	activeReplica int32
	activeHealthy bool

	activeScriptVersion string

	// rule is a value copy of the candidate rule captured at submit time, and
	// configGeneration pins the configuration version. Workers never read the live
	// configuration through the task, which prevents old results from being attached
	// to a new script version.
	rule             *compiledRule
	configGeneration int64
}
