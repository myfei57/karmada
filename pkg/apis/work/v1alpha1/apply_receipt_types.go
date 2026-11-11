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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// MaxWorkApplyReceiptMessageLength caps the human-readable message of a single receipt.
	// Messages are truncated to this length; digests and UIDs are never truncated.
	MaxWorkApplyReceiptMessageLength = 1024
	// MaxMutationSummaryLength caps the serialized mutation summary carried by a receipt.
	MaxMutationSummaryLength = 1024
	// MaxWorkApplyReceiptConfirmedFields caps the number of confirmed field entries per receipt.
	MaxWorkApplyReceiptConfirmedFields = 16
	// MaxWorkApplyReceiptBytes caps the serialized size of a single receipt. The execution
	// controller trims message and confirmed-field entries when the budget is exceeded.
	MaxWorkApplyReceiptBytes = 4096
	// WorkApplyReceiptDigestPrefix is the prefix of the content digest stored in receipts.
	WorkApplyReceiptDigestPrefix = "sha256:"
)

// WorkApplyReceiptPhase represents the per-manifest completion phase recorded by a receipt.
type WorkApplyReceiptPhase string

const (
	// WorkApplyReceiptPhaseApplied means the member cluster object was confirmed by a real
	// write response or a precondition-checked readback.
	WorkApplyReceiptPhaseApplied WorkApplyReceiptPhase = "Applied"
	// WorkApplyReceiptPhaseUnknown means the manifest could not be confirmed (apply error,
	// ownership conflict or an ambiguous write result). It never implies a successful write.
	WorkApplyReceiptPhaseUnknown WorkApplyReceiptPhase = "Unknown"
)

// WorkApplyOperation describes the member cluster operation that produced the receipt evidence.
type WorkApplyOperation string

const (
	// WorkApplyOperationCreated means the object was created through a member API Create call
	// and the response object is recorded as evidence.
	WorkApplyOperationCreated WorkApplyOperation = "Created"
	// WorkApplyOperationUpdated means the object was changed through a member API Update call
	// and the response object is recorded as evidence.
	WorkApplyOperationUpdated WorkApplyOperation = "Updated"
	// WorkApplyOperationUnchanged means an Update call returned without changing the object.
	WorkApplyOperationUnchanged WorkApplyOperation = "Unchanged"
	// WorkApplyOperationObserved means no write happened in this reconciliation; the evidence
	// comes exclusively from a direct server readback that passed identity and ownership checks.
	WorkApplyOperationObserved WorkApplyOperation = "Observed"
)

// Machine-readable reasons explaining why a receipt is in a given phase.
const (
	// WorkApplyReceiptReasonApplied marks a receipt backed by a confirmed write response.
	WorkApplyReceiptReasonApplied = "Applied"
	// WorkApplyReceiptReasonObserved marks a receipt backed by a verified readback.
	WorkApplyReceiptReasonObserved = "Observed"
	// WorkApplyReceiptReasonApplyError marks an Unknown receipt caused by a member API apply error.
	WorkApplyReceiptReasonApplyError = "ApplyError"
	// WorkApplyReceiptReasonOwnershipConflict marks an Unknown receipt caused by an existing object
	// that is either not managed by Karmada or references a different Work identity.
	WorkApplyReceiptReasonOwnershipConflict = "OwnershipConflict"
	// WorkApplyReceiptReasonResultUnknown marks an Unknown receipt caused by an ambiguous write
	// result (for example a timeout) that could not be resolved through readback.
	WorkApplyReceiptReasonResultUnknown = "ResultUnknown"
	// WorkApplyReceiptReasonManifestInvalid marks an Unknown receipt caused by a manifest that
	// could not be parsed or mapped.
	WorkApplyReceiptReasonManifestInvalid = "ManifestInvalid"
)

// Aspects of the desired object that were verified against the observed member cluster object.
const (
	// WorkApplyConfirmedIdentity confirms GVK/namespace/name identity.
	WorkApplyConfirmedIdentity = "identity"
	// WorkApplyConfirmedOwnership confirms Karmada ownership labels and Work reference annotations.
	WorkApplyConfirmedOwnership = "ownership"
	// WorkApplyConfirmedLabels confirms managed labels match the desired object.
	WorkApplyConfirmedLabels = "labels"
	// WorkApplyConfirmedAnnotations confirms managed annotations match the desired object.
	WorkApplyConfirmedAnnotations = "annotations"
	// WorkApplyConfirmedContent confirms pruned and redacted content equality (digest level).
	WorkApplyConfirmedContent = "content"
)

// WorkApplyReceipt is point-in-time evidence that a single Work manifest was written to,
// or observed on, the target member cluster.
//
// A receipt only proves that one specific write or observation happened with the recorded
// object identity and resource version. It does NOT assert the workload is Healthy and it
// does NOT guarantee that the object will not drift afterwards.
type WorkApplyReceipt struct {
	// Identifier links the receipt to a manifest in spec.workload.manifests by ordinal and identity.
	// +required
	Identifier ResourceIdentifier `json:"identifier"`

	// WorkUID is the Work object UID pinned when the evidence was produced. A receipt whose
	// WorkUID differs from the current Work object can never describe the current Work.
	// +required
	WorkUID types.UID `json:"workUID"`

	// WorkGeneration is the Work .metadata.generation pinned when the evidence was produced.
	// Receipts that do not match the current Work generation are pruned and late responses
	// belonging to an older generation can never fill current-generation receipts.
	// +required
	WorkGeneration int64 `json:"workGeneration"`

	// ClusterUID is the .metadata.uid of the Cluster object when the evidence was produced.
	// It changes when the Cluster resource is rebuilt.
	// +required
	ClusterUID types.UID `json:"clusterUID"`

	// ClusterID is the member cluster identity collected during registration (Cluster.spec.id).
	// It is recorded alongside ClusterUID to distinguish member cluster rebuilds.
	// +optional
	ClusterID string `json:"clusterID,omitempty"`

	// ManifestIdentity is the canonical "group/version/kind/namespace/name" identity of the manifest.
	// +required
	ManifestIdentity string `json:"manifestIdentity"`

	// RequestDigest is the digest of the original request manifest before webhook defaulting
	// and mutation (Secret plaintext fields are removed before digesting). It is empty when the
	// Work predates the feature and no webhook summary is available.
	// +optional
	// +kubebuilder:validation:MaxLength=72
	RequestDigest string `json:"requestDigest,omitempty"`

	// AppliedDigest is the digest of the manifest actually dispatched to the member cluster,
	// i.e. after pruning, defaulting and Karmada label/annotation injection (Secret plaintext
	// fields are removed before digesting).
	// +required
	// +kubebuilder:validation:MaxLength=72
	AppliedDigest string `json:"appliedDigest"`

	// MutationSummary summarizes how the webhook transformed the original request, recorded
	// separately from the request itself.
	// +optional
	MutationSummary *ManifestMutationSummary `json:"mutationSummary,omitempty"`

	// ObservedObject references the member cluster object returned by the real write response
	// or a precondition-checked readback. It is nil for Unknown receipts and its UID/resourceVersion
	// must never be populated from informer cache objects.
	// +optional
	ObservedObject *ObservedObjectReference `json:"observedObject,omitempty"`

	// Operation is the member cluster operation that produced the evidence.
	// +required
	// +kubebuilder:validation:Enum=Created;Updated;Unchanged;Observed
	Operation WorkApplyOperation `json:"operation"`

	// Readback indicates ObservedObject was obtained from a direct member API GET under
	// identity and ownership preconditions instead of a write response.
	// +optional
	Readback bool `json:"readback,omitempty"`

	// ConfirmedFields lists the aspects verified against the observed object.
	// +optional
	// +kubebuilder:validation:MaxItems=16
	ConfirmedFields []string `json:"confirmedFields,omitempty"`

	// SecretRedacted is true when the manifest is a Secret: plaintext fields were removed
	// before digesting and are never stored anywhere in the receipt.
	// +optional
	SecretRedacted bool `json:"secretRedacted,omitempty"`

	// Phase is the per-manifest completion phase.
	// +required
	// +kubebuilder:validation:Enum=Applied;Unknown
	Phase WorkApplyReceiptPhase `json:"phase"`

	// Reason is a CamelCase machine-readable reason, particularly required for Unknown receipts.
	// +optional
	// +kubebuilder:validation:MaxLength=320
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable, length-capped message. It must never contain Secret plaintext.
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Message string `json:"message,omitempty"`

	// ObservedAt is the time when the write response or readback was observed by the controller.
	// +required
	ObservedAt metav1.Time `json:"observedAt"`
}

// ManifestMutationSummary records how the mutating webhook transformed the original request
// manifest, separately from the original request content.
type ManifestMutationSummary struct {
	// AddedLabels lists label keys added by Karmada defaulting or injection.
	// +optional
	AddedLabels []string `json:"addedLabels,omitempty"`

	// AddedAnnotations lists annotation keys added by Karmada defaulting or injection.
	// +optional
	AddedAnnotations []string `json:"addedAnnotations,omitempty"`

	// RemovedFields lists field paths pruned because they are server-managed or irrelevant.
	// +optional
	RemovedFields []string `json:"removedFields,omitempty"`

	// WebhookSummaryUnavailable indicates the Work was created before the feature and the
	// webhook did not record a request-side summary.
	// +optional
	WebhookSummaryUnavailable bool `json:"webhookSummaryUnavailable,omitempty"`
}

// ObservedObjectReference holds the server-confirmed identity of the target member cluster object.
type ObservedObjectReference struct {
	// UID is the .metadata.uid returned by the member API.
	// +required
	UID types.UID `json:"uid"`

	// ResourceVersion is the .metadata.resourceVersion returned by the member API.
	// +required
	ResourceVersion string `json:"resourceVersion"`

	// ObservedGeneration is the .metadata.generation returned by the member API, when available.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}
