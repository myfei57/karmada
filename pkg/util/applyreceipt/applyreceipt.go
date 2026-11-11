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

// Package applyreceipt contains pure helpers shared by the Work mutating webhook
// and the execution controller to build digests, redact Secret plaintext and
// serialize mutation summaries for Work apply receipts.
//
// It must not depend on any Kubernetes client so that it stays free of import cycles.
package applyreceipt

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/util"
)

// secretPlaintextFields are the root-level Secret fields that may carry plaintext
// or base64-encoded sensitive values. They must be removed before digesting and
// must never be copied into a Work apply receipt.
var secretPlaintextFields = []string{"data", "stringData", "binaryData"}

// IsSecret reports whether the object is a core (empty group) v1 Secret.
func IsSecret(obj *unstructured.Unstructured) bool {
	gvk := obj.GroupVersionKind()
	return gvk.Group == "" && gvk.Version == "v1" && gvk.Kind == util.SecretKind
}

// Redact returns a deep copy of obj whose Secret plaintext fields are removed.
// Non-Secret objects are returned as an independent deep copy.
func Redact(obj *unstructured.Unstructured) *unstructured.Unstructured {
	redacted := obj.DeepCopy()
	if !IsSecret(redacted) {
		return redacted
	}
	for _, field := range secretPlaintextFields {
		unstructured.RemoveNestedField(redacted.Object, field)
	}
	return redacted
}

// canonicalJSON remarshals raw JSON through the generic representation so that
// object keys are sorted and the digest is independent of source key ordering.
func canonicalJSON(raw []byte) ([]byte, error) {
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, fmt.Errorf("failed to canonicalize JSON: %w", err)
	}
	canonical, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("failed to canonicalize JSON: %w", err)
	}
	return canonical, nil
}

// DigestBytes returns the sha256 digest of the canonical form of raw.
// The returned string is prefixed with workv1alpha1.WorkApplyReceiptDigestPrefix.
func DigestBytes(raw []byte) (string, error) {
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return workv1alpha1.WorkApplyReceiptDigestPrefix + hex.EncodeToString(sum[:]), nil
}

// DigestObject returns the sha256 digest of the redacted, canonical JSON of obj.
// Secret plaintext fields never influence the digest.
func DigestObject(obj *unstructured.Unstructured) (string, error) {
	redacted := Redact(obj)
	raw, err := redacted.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("failed to marshal object for digest: %w", err)
	}
	return DigestBytes(raw)
}

// receiptBookkeepingAnnotations describe the digest/mutation evidence itself and therefore
// must be excluded from content digests to keep them stable across webhook re-admissions.
var receiptBookkeepingAnnotations = []string{
	workv1alpha2.WorkRequestDigestAnnotation,
	workv1alpha2.WorkAppliedDigestAnnotation,
	workv1alpha2.WorkMutationSummaryAnnotation,
}

// DigestForReceipt returns the content digest of obj after redacting Secret plaintext and
// removing receipt bookkeeping annotations. The webhook and the execution controller must use
// this same helper so the stored manifest digest and the post-admission digest stay comparable.
func DigestForReceipt(obj *unstructured.Unstructured) (string, error) {
	redacted := Redact(obj)
	annotations := redacted.GetAnnotations()
	if len(annotations) == 0 {
		return DigestObject(redacted)
	}
	cloned := false
	for _, key := range receiptBookkeepingAnnotations {
		if _, exists := annotations[key]; exists {
			if !cloned {
				annotations = maps.Clone(annotations)
				cloned = true
			}
			delete(annotations, key)
		}
	}
	if cloned {
		if len(annotations) == 0 {
			redacted.SetAnnotations(nil)
		} else {
			redacted.SetAnnotations(annotations)
		}
	}
	return DigestObject(redacted)
}

// ManifestIdentity returns the canonical "group/version/kind/namespace/name" identity.
// The group segment is empty for core resources.
func ManifestIdentity(obj *unstructured.Unstructured) string {
	gvk := obj.GroupVersionKind()
	return fmt.Sprintf("%s/%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, obj.GetNamespace(), obj.GetName())
}

// BuildIdentifier builds a ResourceIdentifier linking a receipt to a Work manifest ordinal.
func BuildIdentifier(ordinal int, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) workv1alpha1.ResourceIdentifier {
	return workv1alpha1.ResourceIdentifier{
		Ordinal:   ordinal,
		Group:     gvr.Group,
		Version:   gvr.Version,
		Kind:      obj.GetKind(),
		Resource:  gvr.Resource,
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

// SortedStringKeys returns the keys of m in deterministic, ascending order.
func SortedStringKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TruncateString caps s to maxRunes runes. It never splits a multi-byte rune.
// Non-positive maxRunes returns the original string unchanged.
func TruncateString(s string, maxRunes int) string {
	if maxRunes <= 0 || len(s) <= maxRunes {
		return s
	}
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes])
}

// MarshalMutationSummary serializes summary as JSON while guaranteeing the output
// does not exceed maxBytes. When necessary, slice entries are dropped from the end
// until the size limit is satisfied; digests and UIDs stored elsewhere are never
// affected because summaries only contain short key names and flags.
func MarshalMutationSummary(summary *workv1alpha1.ManifestMutationSummary, maxBytes int) (string, error) {
	if summary == nil || maxBytes <= 0 {
		return "", nil
	}

	shrink := func(copy *workv1alpha1.ManifestMutationSummary, limit int) *workv1alpha1.ManifestMutationSummary {
		copy.AddedLabels = trimSlice(copy.AddedLabels, limit)
		copy.AddedAnnotations = trimSlice(copy.AddedAnnotations, limit)
		copy.RemovedFields = trimSlice(copy.RemovedFields, limit)
		return copy
	}

	maxEntries := len(summary.AddedLabels)
	for _, entries := range [][]string{summary.AddedAnnotations, summary.RemovedFields} {
		if len(entries) > maxEntries {
			maxEntries = len(entries)
		}
	}

	working := summary.DeepCopy()
	for {
		raw, err := json.Marshal(working)
		if err != nil {
			return "", fmt.Errorf("failed to marshal mutation summary: %w", err)
		}
		if len(raw) <= maxBytes {
			return string(raw), nil
		}
		if maxEntries == 0 {
			// Only fixed-size fields remain; fall back to a hard byte cap.
			candidate := string(raw)
			if len(candidate) > maxBytes {
				return candidate[:maxBytes], nil
			}
			return candidate, nil
		}
		maxEntries--
		working = shrink(working, maxEntries)
	}
}

func trimSlice(entries []string, limit int) []string {
	if limit < 0 {
		limit = 0
	}
	if len(entries) <= limit {
		return entries
	}
	return entries[:limit]
}

// EstimateReceiptBytes returns the serialized JSON size of a receipt. It returns 0
// for nil receipts so callers can treat missing receipts as zero cost.
func EstimateReceiptBytes(receipt *workv1alpha1.WorkApplyReceipt) int {
	if receipt == nil {
		return 0
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return 0
	}
	return len(raw)
}
