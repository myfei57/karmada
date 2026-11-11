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

// Package statuscoverage produces read-only, per-cluster and per-field
// coverage reports for the statuses already collected by Karmada.
//
// The report protocol defined here is intentionally separate from the legacy
// AggregatedStatusItem health fields: it never mutates ResourceBinding,
// ClusterResourceBinding or Work objects, and its integrity fields (cluster
// UID, member object UID, generations) are not mixed into the legacy health
// protocol.
package statuscoverage

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// FieldCoverageState is the coverage state of a single field on a single
// target cluster.
type FieldCoverageState string

const (
	// FieldStateValid means the field was observed and its value is a real,
	// non-zero value that corresponds to the current resource template
	// generation.
	FieldStateValid FieldCoverageState = "Valid"

	// FieldStateValidZero means the field was observed and its value is a
	// genuine zero. It must not be confused with FieldStateMissing: a missing
	// item carries no value at all and can never be counted as zero.
	FieldStateValidZero FieldCoverageState = "ValidZero"

	// FieldStateMissing means the cluster is currently targeted by the
	// binding but no current status evidence was returned for it, either
	// because no AggregatedStatusItem exists, its status is empty, or its
	// identity evidence does not match the current target.
	FieldStateMissing FieldCoverageState = "Missing"

	// FieldStateStaleGeneration means status evidence exists but it
	// corresponds to an older resource template generation, so its values
	// must not be aggregated into current totals.
	FieldStateStaleGeneration FieldCoverageState = "StaleGeneration"

	// FieldStateClusterUnreachable means the member cluster is not ready
	// and fresh status cannot be obtained from it.
	FieldStateClusterUnreachable FieldCoverageState = "ClusterUnreachable"

	// FieldStateInterpreterError means the reflected status payload cannot be
	// parsed for the field, typically because the interpreter returned an
	// unexpected payload.
	FieldStateInterpreterError FieldCoverageState = "InterpreterError"

	// FieldStateNotRequested means the field is explicitly not requested for
	// the resource (for example a Job-only field on a Deployment, or a field
	// filtered out by the report options). It is distinct from Missing: no
	// value is expected here.
	FieldStateNotRequested FieldCoverageState = "NotRequested"
)

// Machine-readable reasons that further explain a coverage state.
const (
	// ReasonClusterNotReady means the cluster Ready condition is not True.
	ReasonClusterNotReady = "ClusterNotReady"
	// ReasonClusterNotFound means the Cluster object does not exist.
	ReasonClusterNotFound = "ClusterNotFound"
	// ReasonNoEvidence means no AggregatedStatusItem was returned for the target.
	ReasonNoEvidence = "NoStatusItem"
	// ReasonNoReflectedStatus means the item carries no reflected status payload.
	ReasonNoReflectedStatus = "NoReflectedStatus"
	// ReasonClusterUIDMismatch means cluster UID evidence does not match the current target.
	ReasonClusterUIDMismatch = "ClusterUIDMismatch"
	// ReasonMemberObjectUIDMismatch means the member object UID does not match the expected one.
	ReasonMemberObjectUIDMismatch = "MemberObjectUIDMismatch"
	// ReasonStaleGeneration means the evidence describes an older resource template generation.
	ReasonStaleGeneration = "StaleGeneration"
	// ReasonInterpretError means the reflected status payload could not be parsed.
	ReasonInterpretError = "InterpretError"
	// ReasonNotRequested means the field is explicitly not requested for the resource.
	ReasonNotRequested = "NotRequested"
)

// ClusterIdentity identifies a member cluster by name and UID. The UID is
// required to tell a rebuilt same-name cluster apart from the original one.
type ClusterIdentity struct {
	// Name is the member cluster name as referenced by the binding.
	Name string `json:"name"`
	// UID is the metadata.uid of the Cluster object. It may be empty when the
	// caller cannot provide it, in which case identity cannot be fully
	// verified and the report marks the entry accordingly.
	// +optional
	UID types.UID `json:"uid,omitempty"`
}

// MemberObjectIdentity identifies the member object behind a status item.
type MemberObjectIdentity struct {
	// UID is the metadata.uid of the object inside the member cluster. A
	// same-name rebuild produces a different UID and must never be mistaken
	// for the current object.
	// +optional
	UID types.UID `json:"uid,omitempty"`

	// ResourceVersion is the opaque metadata.resourceVersion of the member
	// object. It is recorded for string-equality checks only. It MUST NOT be
	// parsed as a number or used to order results across clusters.
	// +optional
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

// ResourceTemplateInfo describes the resource template the report is built
// for. The target universe always comes from the binding spec rather than
// from the returned status items.
type ResourceTemplateInfo struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name"`
	// UID is the metadata.uid of the resource template in the Karmada control
	// plane.
	// +optional
	UID types.UID `json:"uid,omitempty"`
	// Generation is the metadata.generation of the current resource template.
	Generation int64 `json:"generation,omitempty"`
}

// Target is one fixed coverage target derived from the current binding spec.
// The full target set is an explicit input and is never inferred from the
// observed status items.
type Target struct {
	// Identity is the target cluster identity.
	Identity ClusterIdentity `json:"identity"`

	// DesiredReplicas is the replica assignment of this cluster from
	// spec.clusters[*].replicas. Zero means no replica assignment is known.
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`

	// ExpectedMemberUID, when set, is the member object UID previously
	// observed for this target. Evidence carrying a different UID describes a
	// rebuilt same-name object and is not accepted as current evidence.
	// +optional
	ExpectedMemberUID types.UID `json:"expectedMemberUID,omitempty"`

	// AllocatedCompletions is the per-cluster Job completions allocation,
	// re-derived from the current binding. It is only populated for Jobs
	// whose completions are divided across clusters.
	// +optional
	AllocatedCompletions *int32 `json:"allocatedCompletions,omitempty"`
}

// Evidence is the observed status evidence for one cluster. It wraps the
// legacy AggregatedStatusItem and adds the integrity fields that the legacy
// protocol does not carry, keeping the two protocols separated.
type Evidence struct {
	// ClusterName is the cluster this evidence belongs to.
	ClusterName string `json:"clusterName"`
	// ClusterUID is the metadata.uid of the cluster that produced the
	// evidence.
	// +optional
	ClusterUID types.UID `json:"clusterUID,omitempty"`
	// Member is the identity of the member object that produced the status.
	// +optional
	Member MemberObjectIdentity `json:"member,omitempty"`
	// Item is the legacy AggregatedStatusItem. It may be nil when no item has
	// been returned for the cluster.
	// +optional
	Item *AggregatedStatusItem `json:"item,omitempty"`
}

// AggregatedStatusItem is a minimal, protocol-local view of
// workv1alpha2.AggregatedStatusItem. The builder does not depend on the full
// work API types, which keeps the coverage protocol self-contained.
type AggregatedStatusItem struct {
	// Applied mirrors AggregatedStatusItem.Applied.
	Applied bool `json:"applied,omitempty"`
	// AppliedMessage mirrors AggregatedStatusItem.AppliedMessage and is
	// truncated before being stored in the report.
	// +optional
	AppliedMessage string `json:"appliedMessage,omitempty"`
	// StatusRaw is the raw JSON of AggregatedStatusItem.Status.
	// +optional
	StatusRaw []byte `json:"statusRaw,omitempty"`
}

// FieldCoverage is the coverage of one field on one target cluster.
type FieldCoverage struct {
	// State is the coverage state.
	State FieldCoverageState `json:"state"`
	// Value holds the observed value when State is Valid or ValidZero. It is
	// nil in every other state.
	// +optional
	Value *int64 `json:"value,omitempty"`
	// Reason is a machine-readable explanation of non-valid states.
	// +optional
	Reason string `json:"reason,omitempty"`
	// Detail is a short human-readable explanation.
	// +optional
	Detail string `json:"detail,omitempty"`
	// ObservedTemplateGeneration is the resource template generation the
	// evidence corresponds to. It is zero when generation evidence is absent.
	// +optional
	ObservedTemplateGeneration int64 `json:"observedTemplateGeneration,omitempty"`
}

// ClusterCoverage is the per-cluster section of a report.
type ClusterCoverage struct {
	// Cluster is the target cluster identity, including its UID.
	Cluster ClusterIdentity `json:"cluster"`
	// Reachable reports whether the cluster Ready condition is True.
	Reachable bool `json:"reachable"`
	// MemberObject is the identity of the observed member object, if any.
	// +optional
	MemberObject *MemberObjectIdentity `json:"memberObject,omitempty"`
	// IdentityVerified is true only when both the cluster UID and, when an
	// expected member UID is known, the member object UID match.
	IdentityVerified bool `json:"identityVerified"`
	// Fields holds the per-field coverage keyed by field name.
	Fields map[string]FieldCoverage `json:"fields"`
}

// StrictTotal is the strict-all exact total.
type StrictTotal struct {
	// Complete is true only when every target cluster reports a current
	// value (Valid or ValidZero) for the field. A partial view is never
	// reported as complete.
	Complete bool `json:"complete"`
	// Value is the exact sum when Complete is true. It is nil whenever the
	// total cannot be proven exact, in which case callers MUST use the
	// partial bounds instead of assuming zero.
	// +optional
	Value *int64 `json:"value,omitempty"`
}

// BoundedTotal is the explicit partial view of a field total.
type BoundedTotal struct {
	// Lower is the sum of all currently observed values. Missing, stale,
	// unreachable or failed clusters contribute nothing to Lower; they are
	// never implicitly counted as zero.
	Lower int64 `json:"lower"`
	// Upper is a trustworthy upper bound. It is nil when the uncovered
	// quantity has no trustworthy upper bound, which means the upper bound
	// is Unknown.
	// +optional
	Upper *int64 `json:"upper,omitempty"`
	// Unknown is true when at least one target cluster lacks current
	// coverage for the field.
	Unknown bool `json:"unknown"`
}

// FieldCoverageSummary aggregates coverage of one field across all targets.
type FieldCoverageSummary struct {
	// Field is the status field name.
	Field string `json:"field"`
	// Strict is the strict-all exact total.
	Strict StrictTotal `json:"strict"`
	// Partial is the explicit partial lower/upper bound.
	Partial BoundedTotal `json:"partial"`
	// CoveredClusters counts clusters in Valid or ValidZero state.
	CoveredClusters int `json:"coveredClusters"`
	// ValidZeroClusters counts clusters whose observed value is a genuine
	// zero.
	ValidZeroClusters int `json:"validZeroClusters"`
	// MissingClusters lists clusters in Missing state (capped).
	// +optional
	MissingClusters []string `json:"missingClusters,omitempty"`
	// StaleClusters lists clusters in StaleGeneration state (capped).
	// +optional
	StaleClusters []string `json:"staleClusters,omitempty"`
	// UnreachableClusters lists clusters in ClusterUnreachable state
	// (capped).
	// +optional
	UnreachableClusters []string `json:"unreachableClusters,omitempty"`
	// InterpreterErrorClusters lists clusters in InterpreterError state
	// (capped).
	// +optional
	InterpreterErrorClusters []string `json:"interpreterErrorClusters,omitempty"`
}

// StrictGateState reports the result of the optional strict gate.
type StrictGateState struct {
	// Enabled is true only when the gate was explicitly configured by the
	// caller and allowed by the corresponding feature gate.
	Enabled bool `json:"enabled"`
	// Pass is true when Enabled and every requested field has a complete
	// strict total.
	Pass bool `json:"pass,omitempty"`
	// Reason explains why the gate is disabled or failing.
	// +optional
	Reason string `json:"reason,omitempty"`
	// FailedFields lists the fields whose strict total is not complete
	// (capped).
	// +optional
	FailedFields []string `json:"failedFields,omitempty"`
}

// StatusCoverageReport is the read-only coverage report. It is built from a
// fixed target set and existing AggregatedStatusItems; it never changes the
// default aggregator state.
type StatusCoverageReport struct {
	// Resource is the resource template the report describes.
	Resource ResourceTemplateInfo `json:"resource"`
	// GeneratedAt is the report generation time.
	GeneratedAt metav1.Time `json:"generatedAt"`
	// Targets is the number of fixed targets taken from the binding spec.
	Targets int `json:"targets"`
	// StrictGate reports the optional strict gate state.
	StrictGate StrictGateState `json:"strictGate"`
	// Clusters holds the per-cluster coverage and is capped by
	// Options.MaxTargets.
	// +optional
	Clusters []ClusterCoverage `json:"clusters,omitempty"`
	// Fields holds the per-field totals and is capped by Options.MaxFields.
	// +optional
	Fields []FieldCoverageSummary `json:"fields,omitempty"`
	// ExtraneousClusters lists clusters that return evidence but are no
	// longer in the fixed target set (for example after placement removal).
	// Their values are never aggregated. The list is capped.
	// +optional
	ExtraneousClusters []string `json:"extraneousClusters,omitempty"`
	// Truncated is true when one of the output caps was hit.
	Truncated bool `json:"truncated"`
	// Errors holds non-fatal integrity errors (capped by Options.MaxErrors).
	// It never contains secret status content.
	// +optional
	Errors []string `json:"errors,omitempty"`
}
