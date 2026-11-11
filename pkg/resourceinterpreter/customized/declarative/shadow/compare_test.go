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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
	"k8s.io/apimachinery/pkg/util/sets"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
)

func TestCompareReplicas(t *testing.T) {
	t.Run("exact match", func(t *testing.T) {
		requires := &workv1alpha2.ReplicaRequirements{}
		result := compareReplicas(3, requires, requires, 3)
		assert.Equal(t, OutcomeMatch, result.outcome)
	})

	t.Run("replica divergence", func(t *testing.T) {
		result := compareReplicas(3, nil, nil, 4)
		assert.Equal(t, OutcomeDiff, result.outcome)
		assert.Contains(t, result.diffFields, "replicas")
	})

	t.Run("requirements divergence", func(t *testing.T) {
		result := compareReplicas(3, &workv1alpha2.ReplicaRequirements{}, nil, 3)
		assert.Equal(t, OutcomeDiff, result.outcome)
		assert.Contains(t, result.diffFields, "replicaRequirements")
	})
}

func TestCompareHealth(t *testing.T) {
	outcome, fields := compareHealth(true, true)
	assert.Equal(t, OutcomeMatch, outcome)
	assert.Empty(t, fields)

	outcome, fields = compareHealth(true, false)
	assert.Equal(t, OutcomeDiff, outcome)
	assert.Equal(t, []string{"healthy"}, fields)
}

func TestCompareRevisedObject(t *testing.T) {
	base := `{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo"},"spec":{"replicas":3,"image":"nginx"}}`

	t.Run("identical objects match", func(t *testing.T) {
		result := compareRevisedObject([]byte(base), []byte(base), nil)
		assert.Equal(t, OutcomeMatch, result.outcome)
	})

	t.Run("identity change is never allowed", func(t *testing.T) {
		candidate := `{"apiVersion":"v1","kind":"Workload","metadata":{"name":"hacked"},"spec":{"replicas":3,"image":"nginx"}}`
		allowed := sets.New("metadata.name", "spec.replicas")
		result := compareRevisedObject([]byte(base), []byte(candidate), allowed)
		assert.Equal(t, OutcomeIdentityViolation, result.outcome)
		assert.Contains(t, result.diffFields, "metadata.name")
	})

	t.Run("allowed scalar diff", func(t *testing.T) {
		candidate := `{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo"},"spec":{"replicas":5,"image":"nginx"}}`
		result := compareRevisedObject([]byte(base), []byte(candidate), sets.New("spec.replicas"))
		assert.Equal(t, OutcomeAllowedDiff, result.outcome)
		assert.Equal(t, []string{"spec.replicas"}, result.diffFields)
	})

	t.Run("unallowed scalar diff", func(t *testing.T) {
		candidate := `{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo"},"spec":{"replicas":5,"image":"redis"}}`
		result := compareRevisedObject([]byte(base), []byte(candidate), sets.New("spec.replicas"))
		assert.Equal(t, OutcomeDiff, result.outcome)
		assert.Contains(t, result.diffFields, "spec.image")
	})

	t.Run("nested and array diffs", func(t *testing.T) {
		active := `{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo"},"spec":{"containers":[{"name":"a","image":"nginx"}]}}`
		candidate := `{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo"},"spec":{"containers":[{"name":"a","image":"redis"}]}}`
		result := compareRevisedObject([]byte(active), []byte(candidate), nil)
		assert.Equal(t, OutcomeDiff, result.outcome)
		assert.Contains(t, result.diffFields, "spec.containers.0.image")
	})

	t.Run("array length change surfaces the array path", func(t *testing.T) {
		active := `{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo"},"spec":{"items":[1,2]}}`
		candidate := `{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo"},"spec":{"items":[1]}}`
		result := compareRevisedObject([]byte(active), []byte(candidate), nil)
		assert.Equal(t, OutcomeDiff, result.outcome)
		assert.Contains(t, result.diffFields, "spec.items")
	})
}

func TestCollectIdentityViolations(t *testing.T) {
	active := gjson.ParseBytes([]byte(`{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo","namespace":"ns","uid":"u1"}}`))
	candidate := gjson.ParseBytes([]byte(`{"apiVersion":"v1","kind":"Workload","metadata":{"name":"demo2","namespace":"ns","uid":"u1"}}`))
	violations := collectIdentityViolations(active, candidate)
	assert.Equal(t, []string{"metadata.name"}, violations)
}
