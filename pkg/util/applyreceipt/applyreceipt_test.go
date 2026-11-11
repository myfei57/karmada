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

package applyreceipt

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
)

func mustUnstructured(t *testing.T, obj any) *unstructured.Unstructured {
	t.Helper()
	raw, err := json.Marshal(obj)
	require.NoError(t, err)
	u := &unstructured.Unstructured{}
	require.NoError(t, u.UnmarshalJSON(raw))
	return u
}

func TestDigestBytesIsCanonicalAndStable(t *testing.T) {
	// Same content with different key order must produce the same digest.
	d1, err := DigestBytes([]byte(`{"b":"2","a":"1","nested":{"z":1,"y":2}}`))
	require.NoError(t, err)
	d2, err := DigestBytes([]byte(`{"nested":{"y":2,"z":1},"a":"1","b":"2"}`))
	require.NoError(t, err)
	assert.Equal(t, d1, d2)
	assert.True(t, strings.HasPrefix(d1, workv1alpha1.WorkApplyReceiptDigestPrefix))

	// Different content must produce a different digest.
	d3, err := DigestBytes([]byte(`{"a":"2"}`))
	require.NoError(t, err)
	assert.NotEqual(t, d1, d3)

	// Digest format: prefix + 64 hex characters.
	hexPart := strings.TrimPrefix(d1, workv1alpha1.WorkApplyReceiptDigestPrefix)
	assert.Len(t, hexPart, 64)
}

func TestSecretDigestIgnoresPlaintext(t *testing.T) {
	secret := func(data map[string][]byte, stringData map[string]string) *unstructured.Unstructured {
		return mustUnstructured(t, &corev1.Secret{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "s"},
			Data:       data,
			StringData: stringData,
		})
	}

	first := secret(map[string][]byte{"token": []byte("secret-1")}, nil)
	second := secret(map[string][]byte{"token": []byte("totally-different")}, map[string]string{"extra": "plain"})
	third := secret(nil, nil)

	d1, err := DigestObject(first)
	require.NoError(t, err)
	d2, err := DigestObject(second)
	require.NoError(t, err)
	d3, err := DigestObject(third)
	require.NoError(t, err)
	assert.Equal(t, d1, d2, "secret plaintext changes must not influence the redacted digest")
	assert.Equal(t, d1, d3)

	// The redacted copies themselves never retain plaintext fields.
	redacted := Redact(second)
	_, hasData, _ := unstructured.NestedMap(redacted.Object, "data")
	_, hasStringData, _ := unstructured.NestedMap(redacted.Object, "stringData")
	assert.False(t, hasData)
	assert.False(t, hasStringData)

	// The original object must stay untouched by redaction.
	_, originalStillHasData, _ := unstructured.NestedMap(second.Object, "data")
	assert.True(t, originalStillHasData)

	serialized, err := json.Marshal(redacted.Object)
	require.NoError(t, err)
	assert.NotContains(t, string(serialized), "plain")
	assert.NotContains(t, string(serialized), "totally-different")
}

func TestRedactLeavesNonSecretUntouched(t *testing.T) {
	cm := mustUnstructured(t, &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cm"},
		Data:       map[string]string{"key": "value"},
	})
	redacted := Redact(cm)
	value, found, err := unstructured.NestedString(redacted.Object, "data", "key")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value", value)
	assert.False(t, IsSecret(cm))
	assert.True(t, IsSecret(mustUnstructured(t, &corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}})))
}

func TestManifestIdentityAndBuildIdentifier(t *testing.T) {
	pod := mustUnstructured(t, &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod"},
	})
	assert.Equal(t, "/v1/Pod/ns/pod", ManifestIdentity(pod))

	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	identifier := BuildIdentifier(2, gvr, pod)
	assert.Equal(t, workv1alpha1.ResourceIdentifier{
		Ordinal:   2,
		Group:     "",
		Version:   "v1",
		Kind:      "Pod",
		Resource:  "pods",
		Namespace: "ns",
		Name:      "pod",
	}, identifier)

	deployment := mustUnstructured(t, map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"namespace": "ns", "name": "deploy"},
	})
	assert.Equal(t, "apps/v1/Deployment/ns/deploy", ManifestIdentity(deployment))
}

func TestSortedStringKeys(t *testing.T) {
	assert.Nil(t, SortedStringKeys(nil))
	assert.Equal(t, []string{"a", "b", "c"}, SortedStringKeys(map[string]string{"c": "1", "a": "2", "b": "3"}))
}

func TestTruncateString(t *testing.T) {
	assert.Equal(t, "abc", TruncateString("abc", 10))
	assert.Equal(t, "ab", TruncateString("abc", 2))
	// Multi-byte runes must not be split.
	assert.Equal(t, "你好", TruncateString("你好世界", 2))
	assert.Equal(t, "abc", TruncateString("abc", 0))
}

func TestMarshalMutationSummaryRespectsByteCap(t *testing.T) {
	summary := &workv1alpha1.ManifestMutationSummary{
		AddedLabels:      []string{"label-a", "label-b", "label-c"},
		AddedAnnotations: []string{"annotation-a", "annotation-b"},
		RemovedFields:    []string{"metadata.uid", "metadata.resourceVersion", "status", "metadata.generation"},
	}

	raw, err := MarshalMutationSummary(summary, workv1alpha1.MaxMutationSummaryLength)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(raw), workv1alpha1.MaxMutationSummaryLength)
	var roundTripped workv1alpha1.ManifestMutationSummary
	require.NoError(t, json.Unmarshal([]byte(raw), &roundTripped))

	// A tiny cap must still produce parseable (or hard-capped) output and never exceed the cap.
	tiny, err := MarshalMutationSummary(summary, 40)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(tiny), 40)

	// Nil and non-positive caps are no-ops.
	out, err := MarshalMutationSummary(nil, 100)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestEstimateReceiptBytes(t *testing.T) {
	assert.Equal(t, 0, EstimateReceiptBytes(nil))
	receipt := &workv1alpha1.WorkApplyReceipt{
		WorkUID:          types.UID("work-uid"),
		WorkGeneration:   1,
		ClusterUID:       types.UID("cluster-uid"),
		AppliedDigest:    "sha256:abc",
		Operation:        workv1alpha1.WorkApplyOperationCreated,
		Phase:            workv1alpha1.WorkApplyReceiptPhaseApplied,
		ManifestIdentity: "/v1/Secret/ns/s",
	}
	assert.Greater(t, EstimateReceiptBytes(receipt), 0)
}
