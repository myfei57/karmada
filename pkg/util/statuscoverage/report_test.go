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

package statuscoverage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/default/native"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/helper"
)

const (
	templateGeneration int64 = 2
	templateUID              = types.UID("template-uid-1")
)

func deploymentInfo() ResourceTemplateInfo {
	return ResourceTemplateInfo{
		APIVersion: "apps/v1",
		Kind:       util.DeploymentKind,
		Namespace:  "default",
		Name:       "nginx",
		UID:        templateUID,
		Generation: templateGeneration,
	}
}

func jobInfo() ResourceTemplateInfo {
	return ResourceTemplateInfo{
		APIVersion: "batch/v1",
		Kind:       util.JobKind,
		Namespace:  "default",
		Name:       "pi",
		UID:        templateUID,
		Generation: templateGeneration,
	}
}

func readyCluster(name string, uid types.UID) clusterv1alpha1.Cluster {
	return clusterv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
		Status: clusterv1alpha1.ClusterStatus{
			Conditions: []metav1.Condition{
				{Type: clusterv1alpha1.ClusterConditionReady, Status: metav1.ConditionTrue},
			},
		},
	}
}

func notReadyCluster(name string, uid types.UID) clusterv1alpha1.Cluster {
	return clusterv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
		Status: clusterv1alpha1.ClusterStatus{
			Conditions: []metav1.Condition{
				{Type: clusterv1alpha1.ClusterConditionReady, Status: metav1.ConditionFalse},
			},
		},
	}
}

func coverageTarget(name string, uid types.UID, desiredReplicas int32) Target {
	return Target{
		Identity:        ClusterIdentity{Name: name, UID: uid},
		DesiredReplicas: desiredReplicas,
	}
}

func deploymentRaw(generation, resourceTemplateGeneration int64, replicas, ready, available, updated, unavailable int32) []byte {
	raw, err := json.Marshal(map[string]any{
		"generation":                 generation,
		"resourceTemplateGeneration": resourceTemplateGeneration,
		"observedGeneration":         generation,
		"replicas":                   replicas,
		"readyReplicas":              ready,
		"availableReplicas":          available,
		"updatedReplicas":            updated,
		"unavailableReplicas":        unavailable,
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func jobRaw(active, succeeded, failed int32) []byte {
	raw, err := json.Marshal(map[string]any{
		"active":    active,
		"succeeded": succeeded,
		"failed":    failed,
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func deploymentEvidence(clusterName string, clusterUID, memberUID types.UID, raw []byte) Evidence {
	return Evidence{
		ClusterName: clusterName,
		ClusterUID:  clusterUID,
		Member:      MemberObjectIdentity{UID: memberUID, ResourceVersion: "rv-" + clusterName},
		Item: &AggregatedStatusItem{
			Applied:   true,
			StatusRaw: raw,
		},
	}
}

func findSummary(t *testing.T, report *StatusCoverageReport, field string) FieldCoverageSummary {
	t.Helper()
	for _, summary := range report.Fields {
		if summary.Field == field {
			return summary
		}
	}
	t.Fatalf("field summary %q not found in report", field)
	return FieldCoverageSummary{}
}

func findClusterCoverage(t *testing.T, report *StatusCoverageReport, name string) ClusterCoverage {
	t.Helper()
	for _, coverage := range report.Clusters {
		if coverage.Cluster.Name == name {
			return coverage
		}
	}
	t.Fatalf("cluster coverage %q not found in report", name)
	return ClusterCoverage{}
}

func int64Ptr(v int64) *int64 { return &v }

// TestBuildDeploymentFullCoverageMatchesNativeAggregation builds a complete
// snapshot where every target returns current evidence and proves the strict
// totals equal the totals produced by the real native aggregate-status
// interpreter, including genuine zero values.
func TestBuildDeploymentFullCoverageMatchesNativeAggregation(t *testing.T) {
	targets := []Target{
		coverageTarget("member-a", "uid-a", 3),
		coverageTarget("member-b", "uid-b", 2),
		coverageTarget("member-c", "uid-c", 1),
	}
	clusters := []clusterv1alpha1.Cluster{
		readyCluster("member-a", "uid-a"),
		readyCluster("member-b", "uid-b"),
		readyCluster("member-c", "uid-c"),
	}
	// member-c reports genuine zeros for ready/available/unavailable.
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-a",
			deploymentRaw(2, templateGeneration, 3, 3, 3, 3, 0)),
		deploymentEvidence("member-b", "uid-b", "member-uid-b",
			deploymentRaw(2, templateGeneration, 2, 1, 1, 2, 1)),
		deploymentEvidence("member-c", "uid-c", "member-uid-c",
			deploymentRaw(2, templateGeneration, 1, 0, 0, 1, 0)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	// Cross-check against the real native aggregate interpreter.
	items := make([]workv1alpha2.AggregatedStatusItem, 0, len(evidences))
	for _, ev := range evidences {
		items = append(items, workv1alpha2.AggregatedStatusItem{
			ClusterName: ev.ClusterName,
			Applied:     true,
			Status:      &runtime.RawExtension{Raw: ev.Item.StatusRaw},
		})
	}
	deploy := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: util.DeploymentKind},
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "nginx", Generation: templateGeneration},
	}
	deployUnstructured, err := helper.ToUnstructured(deploy)
	require.NoError(t, err)
	aggregated, err := native.NewDefaultInterpreter().AggregateStatus(deployUnstructured, items)
	require.NoError(t, err)

	expectations := map[string]int64{
		util.ReplicasField:      6, // 3 + 2 + 1
		util.ReadyReplicasField: 4, // 3 + 1 + 0
		"availableReplicas":     4,
		"updatedReplicas":       6,
		"unavailableReplicas":   1,
	}
	for field, expected := range expectations {
		nativeValue, found, err := unstructured.NestedInt64(aggregated.Object, "status", field)
		require.NoError(t, err)
		require.True(t, found, "native aggregated status missing field %s", field)
		require.Equal(t, expected, nativeValue, "native total mismatch for %s", field)

		summary := findSummary(t, report, field)
		require.True(t, summary.Strict.Complete, "field %s should be fully covered", field)
		require.NotNil(t, summary.Strict.Value, "strict value must be present for full coverage: %s", field)
		assert.Equal(t, expected, *summary.Strict.Value, "strict total for %s must match native aggregation", field)
		assert.False(t, summary.Partial.Unknown)
		require.NotNil(t, summary.Partial.Upper)
		assert.Equal(t, expected, *summary.Partial.Upper)
		assert.Equal(t, expected, summary.Partial.Lower)
	}

	// The genuine zero on member-c must be reported as ValidZero, not Missing.
	memberC := findClusterCoverage(t, report, "member-c")
	assert.Equal(t, FieldStateValidZero, memberC.Fields[util.ReadyReplicasField].State)
	assert.Equal(t, int64(0), *memberC.Fields[util.ReadyReplicasField].Value)
	readySummary := findSummary(t, report, util.ReadyReplicasField)
	assert.Equal(t, 1, readySummary.ValidZeroClusters)
	assert.Equal(t, 3, readySummary.CoveredClusters)
	assert.True(t, memberC.IdentityVerified)
}

// TestBuildDeploymentLostCluster verifies that an unreachable target makes the
// strict total Unknown while keeping explicit partial bounds, and that fields
// without a trustworthy bound report an Unknown upper bound.
func TestBuildDeploymentLostCluster(t *testing.T) {
	targets := []Target{
		coverageTarget("member-a", "uid-a", 3),
		coverageTarget("member-b", "uid-b", 2),
	}
	clusters := []clusterv1alpha1.Cluster{
		readyCluster("member-a", "uid-a"),
		notReadyCluster("member-b", "uid-b"),
	}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-a",
			deploymentRaw(2, templateGeneration, 3, 2, 2, 3, 1)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	memberB := findClusterCoverage(t, report, "member-b")
	assert.False(t, memberB.Reachable)
	for field, fc := range memberB.Fields {
		assert.Equal(t, FieldStateClusterUnreachable, fc.State, "field %s", field)
	}

	ready := findSummary(t, report, util.ReadyReplicasField)
	assert.False(t, ready.Strict.Complete)
	assert.Nil(t, ready.Strict.Value, "strict total must be Unknown, never faked as zero")
	assert.True(t, ready.Partial.Unknown)
	assert.Equal(t, int64(2), ready.Partial.Lower)
	require.NotNil(t, ready.Partial.Upper, "readyReplicas admits a desired-replica upper bound")
	assert.Equal(t, int64(4), *ready.Partial.Upper) // lower 2 + member-b desired 2
	assert.Equal(t, []string{"member-b"}, ready.UnreachableClusters)

	replicas := findSummary(t, report, util.ReplicasField)
	assert.False(t, replicas.Strict.Complete)
	assert.Nil(t, replicas.Strict.Value)
	assert.Nil(t, replicas.Partial.Upper, "current replicas have no trustworthy upper bound")
	assert.Equal(t, int64(3), replicas.Partial.Lower)
}

// TestBuildDeploymentClusterNotFound treats a missing Cluster object as
// unreachable rather than silently dropping the target.
func TestBuildDeploymentClusterNotFound(t *testing.T) {
	targets := []Target{coverageTarget("member-a", "uid-a", 3)}
	report, err := Build(deploymentInfo(), targets, nil, nil, Options{})
	require.NoError(t, err)

	memberA := findClusterCoverage(t, report, "member-a")
	assert.Equal(t, FieldStateClusterUnreachable, memberA.Fields[util.ReplicasField].State)
	assert.Equal(t, ReasonClusterNotFound, memberA.Fields[util.ReplicasField].Reason)
	ready := findSummary(t, report, util.ReadyReplicasField)
	assert.False(t, ready.Strict.Complete)
	assert.Equal(t, int64(0), ready.Partial.Lower)
	assert.Nil(t, ready.Strict.Value)
}

// TestBuildDeploymentRealZero proves a current, parseable zero is a strict
// exact zero, while a missing item never is.
func TestBuildDeploymentRealZero(t *testing.T) {
	targets := []Target{coverageTarget("member-a", "uid-a", 3)}
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a")}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-a",
			deploymentRaw(2, templateGeneration, 0, 0, 0, 0, 0)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	ready := findSummary(t, report, util.ReadyReplicasField)
	require.True(t, ready.Strict.Complete)
	require.NotNil(t, ready.Strict.Value)
	assert.Equal(t, int64(0), *ready.Strict.Value)
	assert.Equal(t, 1, ready.ValidZeroClusters)

	// Same numeric zero but without an item must remain Missing and Unknown.
	reportMissing, err := Build(deploymentInfo(), targets, clusters, nil, Options{})
	require.NoError(t, err)
	readyMissing := findSummary(t, reportMissing, util.ReadyReplicasField)
	assert.False(t, readyMissing.Strict.Complete)
	assert.Nil(t, readyMissing.Strict.Value)
	assert.Equal(t, []string{"member-a"}, readyMissing.MissingClusters)
	assert.Equal(t, int64(0), readyMissing.Partial.Lower)
}

// TestBuildDeploymentStaleGeneration verifies that evidence describing an
// older template generation is excluded from all totals.
func TestBuildDeploymentStaleGeneration(t *testing.T) {
	targets := []Target{
		coverageTarget("member-a", "uid-a", 3),
		coverageTarget("member-b", "uid-b", 2),
	}
	clusters := []clusterv1alpha1.Cluster{
		readyCluster("member-a", "uid-a"),
		readyCluster("member-b", "uid-b"),
	}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-a",
			deploymentRaw(1, 1, 3, 3, 3, 3, 0)), // stale: rtg 1 < template 2
		deploymentEvidence("member-b", "uid-b", "member-uid-b",
			deploymentRaw(2, templateGeneration, 2, 2, 2, 2, 0)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	memberA := findClusterCoverage(t, report, "member-a")
	assert.Equal(t, FieldStateStaleGeneration, memberA.Fields[util.ReadyReplicasField].State)
	assert.Equal(t, int64(1), memberA.Fields[util.ReadyReplicasField].ObservedTemplateGeneration)

	ready := findSummary(t, report, util.ReadyReplicasField)
	assert.False(t, ready.Strict.Complete)
	assert.Nil(t, ready.Strict.Value)
	assert.Equal(t, int64(2), ready.Partial.Lower, "stale values must not enter the lower bound")
	require.NotNil(t, ready.Partial.Upper)
	assert.Equal(t, int64(5), *ready.Partial.Upper) // 2 observed + 3 desired on stale cluster
	assert.Equal(t, []string{"member-a"}, ready.StaleClusters)
}

// TestBuildPlacementRemovalExtraneousEvidence ensures evidence from clusters
// removed from the current placement is never part of the target universe or
// totals.
func TestBuildPlacementRemovalExtraneousEvidence(t *testing.T) {
	targets := []Target{
		coverageTarget("member-a", "uid-a", 3),
		coverageTarget("member-b", "uid-b", 2),
	}
	clusters := []clusterv1alpha1.Cluster{
		readyCluster("member-a", "uid-a"),
		readyCluster("member-b", "uid-b"),
		readyCluster("member-c", "uid-c"),
	}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-a",
			deploymentRaw(2, templateGeneration, 3, 3, 3, 3, 0)),
		deploymentEvidence("member-b", "uid-b", "member-uid-b",
			deploymentRaw(2, templateGeneration, 2, 2, 2, 2, 0)),
		// member-c was removed from placement but its legacy item still exists.
		deploymentEvidence("member-c", "uid-c", "member-uid-c",
			deploymentRaw(2, templateGeneration, 5, 5, 5, 5, 0)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	assert.Equal(t, []string{"member-c"}, report.ExtraneousClusters)
	assert.Equal(t, 2, report.Targets)
	assert.Len(t, report.Clusters, 2)
	ready := findSummary(t, report, util.ReadyReplicasField)
	require.True(t, ready.Strict.Complete)
	require.NotNil(t, ready.Strict.Value)
	assert.Equal(t, int64(5), *ready.Strict.Value, "removed cluster's values must not be aggregated")
}

// TestBuildClusterUIDMismatch simulates a same-name cluster rebuild: the
// Cluster object now carries a different UID and the stale evidence must not
// be accepted as current.
func TestBuildClusterUIDMismatch(t *testing.T) {
	targets := []Target{coverageTarget("member-a", "uid-a-original", 3)}
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a-rebuilt")}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a-original", "member-uid-a",
			deploymentRaw(2, templateGeneration, 3, 3, 3, 3, 0)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	memberA := findClusterCoverage(t, report, "member-a")
	assert.Equal(t, FieldStateMissing, memberA.Fields[util.ReplicasField].State)
	assert.Equal(t, ReasonClusterUIDMismatch, memberA.Fields[util.ReplicasField].Reason)
	assert.False(t, memberA.IdentityVerified)
	require.NotEmpty(t, report.Errors)

	ready := findSummary(t, report, util.ReadyReplicasField)
	assert.False(t, ready.Strict.Complete)
	assert.Nil(t, ready.Strict.Value)
	assert.Equal(t, int64(0), ready.Partial.Lower, "rebuilt cluster evidence must not be counted")
}

// TestBuildEvidenceClusterUIDMismatch covers the case where the Cluster object
// is current but the returned evidence still carries the old cluster UID.
func TestBuildEvidenceClusterUIDMismatch(t *testing.T) {
	targets := []Target{coverageTarget("member-a", "uid-a", 3)}
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a")}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a-old", "member-uid-a",
			deploymentRaw(2, templateGeneration, 3, 3, 3, 3, 0)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	memberA := findClusterCoverage(t, report, "member-a")
	assert.Equal(t, FieldStateMissing, memberA.Fields[util.ReplicasField].State)
	assert.Equal(t, ReasonClusterUIDMismatch, memberA.Fields[util.ReplicasField].Reason)
}

// TestBuildMemberObjectUIDMismatch simulates a same-name member object
// rebuild: the observed member UID no longer matches the expected one.
func TestBuildMemberObjectUIDMismatch(t *testing.T) {
	targets := []Target{
		func() Target {
			t := coverageTarget("member-a", "uid-a", 3)
			t.ExpectedMemberUID = "member-uid-original"
			return t
		}(),
	}
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a")}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-rebuilt",
			deploymentRaw(2, templateGeneration, 3, 3, 3, 3, 0)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	memberA := findClusterCoverage(t, report, "member-a")
	assert.Equal(t, FieldStateMissing, memberA.Fields[util.ReplicasField].State)
	assert.Equal(t, ReasonMemberObjectUIDMismatch, memberA.Fields[util.ReplicasField].Reason)
	assert.Nil(t, readySummaryValue(t, report), "rebuilt member object values must not be counted")
}

func readySummaryValue(t *testing.T, report *StatusCoverageReport) *int64 {
	t.Helper()
	return findSummary(t, report, util.ReadyReplicasField).Strict.Value
}

// TestBuildInterpreterError verifies that an unparsable payload is flagged as
// an interpreter error and never contributes values.
func TestBuildInterpreterError(t *testing.T) {
	targets := []Target{coverageTarget("member-a", "uid-a", 3)}
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a")}
	evidences := []Evidence{{
		ClusterName: "member-a",
		ClusterUID:  "uid-a",
		Member:      MemberObjectIdentity{UID: "member-uid-a"},
		Item:        &AggregatedStatusItem{Applied: true, StatusRaw: []byte("{not-json")},
	}}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	memberA := findClusterCoverage(t, report, "member-a")
	assert.Equal(t, FieldStateInterpreterError, memberA.Fields[util.ReplicasField].State)
	assert.Equal(t, ReasonInterpretError, memberA.Fields[util.ReplicasField].Reason)
	ready := findSummary(t, report, util.ReadyReplicasField)
	assert.False(t, ready.Strict.Complete)
	assert.Equal(t, []string{"member-a"}, ready.InterpreterErrorClusters)
	require.NotEmpty(t, report.Errors)
}

// TestBuildNotRequested verifies the explicit NotRequested state for fields
// filtered out by the report options.
func TestBuildNotRequested(t *testing.T) {
	targets := []Target{coverageTarget("member-a", "uid-a", 3)}
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a")}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-a",
			deploymentRaw(2, templateGeneration, 3, 3, 3, 3, 0)),
	}

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{
		RequestedFields: []string{util.ReplicasField},
	})
	require.NoError(t, err)
	require.Len(t, report.Fields, 1)
	assert.Equal(t, util.ReplicasField, report.Fields[0].Field)

	memberA := findClusterCoverage(t, report, "member-a")
	assert.Equal(t, FieldStateNotRequested, memberA.Fields[util.ReadyReplicasField].State)
	assert.Equal(t, ReasonNotRequested, memberA.Fields[util.ReadyReplicasField].Reason)

	_, err = Build(deploymentInfo(), targets, clusters, evidences, Options{
		RequestedFields: []string{"succeeded"},
	})
	require.Error(t, err)
}

// TestJobDividedCompletionsAllocation verifies that Job completions are
// re-derived from the current binding targets with the same algorithm as the
// binding controller, and that succeeded counts from a removed target never
// enter the new total.
func TestJobDividedCompletionsAllocation(t *testing.T) {
	const templateCompletions int64 = 10
	currentTargets := []Target{
		coverageTarget("member-a", "uid-a", 6),
		coverageTarget("member-b", "uid-b", 4),
	}

	// The helper must agree with the binding controller division algorithm on
	// the current target set.
	allocated := AllocateJobCompletions(templateCompletions, currentTargets, templateUID)
	expectedTCs := helper.SpreadReplicasByTargetClusters(
		int32(templateCompletions), // #nosec G115: integer overflow conversion int64 -> int32
		[]workv1alpha2.TargetCluster{{Name: "member-a", Replicas: 6}, {Name: "member-b", Replicas: 4}},
		nil, templateUID)
	expectedAllocation := map[string]int32{}
	for _, tc := range expectedTCs {
		expectedAllocation[tc.Name] = tc.Replicas
	}
	require.Len(t, allocated, 2)
	for _, tgt := range allocated {
		require.NotNil(t, tgt.AllocatedCompletions)
		assert.Equal(t, expectedAllocation[tgt.Identity.Name], *tgt.AllocatedCompletions)
	}

	clusters := []clusterv1alpha1.Cluster{
		readyCluster("member-a", "uid-a"),
		readyCluster("member-b", "uid-b"),
		readyCluster("member-c", "uid-c"),
	}
	memberAEvidence := Evidence{ClusterName: "member-a", ClusterUID: "uid-a", Member: MemberObjectIdentity{UID: "m-a"},
		Item: &AggregatedStatusItem{Applied: true, StatusRaw: jobRaw(0, expectedAllocation["member-a"], 0)}}
	memberBEvidence := Evidence{ClusterName: "member-b", ClusterUID: "uid-b", Member: MemberObjectIdentity{UID: "m-b"},
		Item: &AggregatedStatusItem{Applied: true, StatusRaw: jobRaw(1, 0, 0)}}
	// member-c belongs to the old placement; its succeeded value is extraneous.
	memberCEvidence := Evidence{ClusterName: "member-c", ClusterUID: "uid-c", Member: MemberObjectIdentity{UID: "m-c"},
		Item: &AggregatedStatusItem{Applied: true, StatusRaw: jobRaw(0, 10, 0)}}

	// Phase 1: member-b has not returned any item, so its contribution is
	// Missing rather than zero.
	evidences := []Evidence{memberAEvidence, memberCEvidence}
	report, err := Build(jobInfo(), allocated, clusters, evidences, Options{})
	require.NoError(t, err)
	assert.Equal(t, []string{"member-c"}, report.ExtraneousClusters)

	succeeded := findSummary(t, report, "succeeded")
	assert.False(t, succeeded.Strict.Complete, "member-b has no succeeded evidence yet")
	assert.Equal(t, int64(expectedAllocation["member-a"]), succeeded.Partial.Lower)
	require.NotNil(t, succeeded.Partial.Upper)
	// Upper bound = observed completions on member-a + member-b allocation.
	assert.Equal(t, int64(expectedAllocation["member-a"])+int64(expectedAllocation["member-b"]),
		*succeeded.Partial.Upper)
	assert.Equal(t, []string{"member-b"}, succeeded.MissingClusters)
	// active has no bound source.
	active := findSummary(t, report, "active")
	assert.Nil(t, active.Partial.Upper)

	// Phase 2: member-b reports its full allocation. The strict total equals
	// the current template completions only, never including member-c's ten.
	memberBEvidence.Item.StatusRaw = jobRaw(0, expectedAllocation["member-b"], 0)
	fullEvidences := []Evidence{memberAEvidence, memberBEvidence, memberCEvidence}
	reportFull, err := Build(jobInfo(), allocated, clusters, fullEvidences, Options{})
	require.NoError(t, err)
	succeededFull := findSummary(t, reportFull, "succeeded")
	require.True(t, succeededFull.Strict.Complete)
	require.NotNil(t, succeededFull.Strict.Value)
	assert.Equal(t, templateCompletions, *succeededFull.Strict.Value)

	// Cross-check with the native Job aggregation over the same snapshot.
	job := &batchv1.Job{
		TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: util.JobKind},
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pi", Generation: templateGeneration},
	}
	jobUnstructured, err := helper.ToUnstructured(job)
	require.NoError(t, err)
	items := []workv1alpha2.AggregatedStatusItem{
		{ClusterName: "member-a", Applied: true, Status: &runtime.RawExtension{Raw: memberAEvidence.Item.StatusRaw}},
		{ClusterName: "member-b", Applied: true, Status: &runtime.RawExtension{Raw: memberBEvidence.Item.StatusRaw}},
	}
	aggregated, err := native.NewDefaultInterpreter().AggregateStatus(jobUnstructured, items)
	require.NoError(t, err)
	nativeSucceeded, found, err := unstructured.NestedInt64(aggregated.Object, "status", "succeeded")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, nativeSucceeded, *succeededFull.Strict.Value)
}

// TestJobDuplicatedCompletions checks duplicated scheduling: every current
// target runs the full template completions and bounds succeed accordingly.
func TestJobDuplicatedCompletions(t *testing.T) {
	const templateCompletions int64 = 5
	targets := DuplicatedJobCompletions(templateCompletions, []Target{
		coverageTarget("member-a", "uid-a", 0),
		coverageTarget("member-b", "uid-b", 0),
	})
	for _, tgt := range targets {
		require.NotNil(t, tgt.AllocatedCompletions)
		assert.Equal(t, int32(templateCompletions), *tgt.AllocatedCompletions)
	}

	clusters := []clusterv1alpha1.Cluster{
		readyCluster("member-a", "uid-a"),
		readyCluster("member-b", "uid-b"),
	}
	evidences := []Evidence{
		{ClusterName: "member-a", ClusterUID: "uid-a", Item: &AggregatedStatusItem{Applied: true, StatusRaw: jobRaw(0, 5, 0)}},
	}
	report, err := Build(jobInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	succeeded := findSummary(t, report, "succeeded")
	assert.False(t, succeeded.Strict.Complete)
	assert.Equal(t, int64(5), succeeded.Partial.Lower)
	require.NotNil(t, succeeded.Partial.Upper)
	assert.Equal(t, int64(10), *succeeded.Partial.Upper) // 5 observed + 5 allocated to member-b
}

// TestJobRealZeroSucceeded verifies genuine zero succeeded is ValidZero and
// produces an exact strict zero when all targets cover it.
func TestJobRealZeroSucceeded(t *testing.T) {
	targets := DuplicatedJobCompletions(3, []Target{coverageTarget("member-a", "uid-a", 0)})
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a")}
	evidences := []Evidence{{
		ClusterName: "member-a", ClusterUID: "uid-a",
		Item: &AggregatedStatusItem{Applied: true, StatusRaw: jobRaw(0, 0, 0)},
	}}
	report, err := Build(jobInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)

	memberA := findClusterCoverage(t, report, "member-a")
	assert.Equal(t, FieldStateValidZero, memberA.Fields["succeeded"].State)
	succeeded := findSummary(t, report, "succeeded")
	require.True(t, succeeded.Strict.Complete)
	require.NotNil(t, succeeded.Strict.Value)
	assert.Equal(t, int64(0), *succeeded.Strict.Value)
}

// TestBuildSecretRejected guarantees Secret status paths are never read.
func TestBuildSecretRejected(t *testing.T) {
	info := ResourceTemplateInfo{APIVersion: "v1", Kind: util.SecretKind, Namespace: "default", Name: "top-secret"}
	report, err := Build(info, []Target{{Identity: ClusterIdentity{Name: "member-a"}}}, nil, nil, Options{})
	require.Error(t, err)
	assert.Nil(t, report)
}

// TestBuildUnsupportedKind reports kinds without a field registry.
func TestBuildUnsupportedKind(t *testing.T) {
	info := ResourceTemplateInfo{APIVersion: "v1", Kind: "ConfigMap", Namespace: "default", Name: "cm"}
	_, err := Build(info, []Target{{Identity: ClusterIdentity{Name: "member-a"}}}, nil, nil, Options{})
	require.Error(t, err)
}

// TestBuildOutputCaps verifies target/error truncation while totals still
// cover every fixed target.
func TestBuildOutputCaps(t *testing.T) {
	const targetCount = 5
	targets := make([]Target, 0, targetCount)
	clusters := make([]clusterv1alpha1.Cluster, 0, targetCount)
	for i := 0; i < targetCount; i++ {
		name := "member-" + string(rune('a'+i))
		uid := types.UID("uid-" + name)
		targets = append(targets, coverageTarget(name, uid, 1))
		clusters = append(clusters, notReadyCluster(name, uid))
	}

	report, err := Build(deploymentInfo(), targets, clusters, nil, Options{MaxTargets: 2, MaxReasonEntries: 2})
	require.NoError(t, err)
	assert.Len(t, report.Clusters, 2)
	assert.True(t, report.Truncated)
	ready := findSummary(t, report, util.ReadyReplicasField)
	assert.Len(t, ready.UnreachableClusters, 2, "reason lists are capped independently")
	assert.Equal(t, targetCount, report.Targets, "the fixed target universe is fully retained")
	assert.False(t, ready.Strict.Complete)
}

// TestStrictGateLifecycle covers the explicit opt-in strict gate and the
// feature gate that must be enabled separately.
func TestStrictGateLifecycle(t *testing.T) {
	original := features.FeatureGate.DeepCopy()
	t.Cleanup(func() { features.FeatureGate = original })

	targets := []Target{coverageTarget("member-a", "uid-a", 3)}
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a")}
	goodEvidence := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-a",
			deploymentRaw(2, templateGeneration, 3, 3, 3, 3, 0)),
	}

	// No explicit config: the gate stays disabled.
	report, err := Build(deploymentInfo(), targets, clusters, goodEvidence, Options{})
	require.NoError(t, err)
	assert.False(t, report.StrictGate.Enabled)
	assert.Equal(t, "NotConfigured", report.StrictGate.Reason)

	// Explicit config but feature gate off: disabled by feature gate.
	report, err = Build(deploymentInfo(), targets, clusters, goodEvidence, Options{StrictGate: &StrictGateConfig{EnforceAll: true}})
	require.NoError(t, err)
	assert.False(t, report.StrictGate.Enabled)
	assert.Equal(t, "FeatureGateDisabled:StatusCoverageReport", report.StrictGate.Reason)

	// Enable the feature gate: full coverage passes.
	require.NoError(t, features.FeatureGate.Set("StatusCoverageReport=true"))
	report, err = Build(deploymentInfo(), targets, clusters, goodEvidence, Options{StrictGate: &StrictGateConfig{EnforceAll: true}})
	require.NoError(t, err)
	assert.True(t, report.StrictGate.Enabled)
	assert.True(t, report.StrictGate.Pass)

	// Partial coverage must never pass the gate or look like a success.
	reportPartial, err := Build(deploymentInfo(), targets, clusters, nil, Options{StrictGate: &StrictGateConfig{EnforceAll: true}})
	require.NoError(t, err)
	assert.True(t, reportPartial.StrictGate.Enabled)
	assert.False(t, reportPartial.StrictGate.Pass)
	assert.Equal(t, "IncompleteCoverage", reportPartial.StrictGate.Reason)
	assert.Contains(t, reportPartial.StrictGate.FailedFields, util.ReadyReplicasField)

	// Audit mode stays enabled but never fails on incomplete coverage.
	reportAudit, err := Build(deploymentInfo(), targets, clusters, nil, Options{StrictGate: &StrictGateConfig{EnforceAll: false}})
	require.NoError(t, err)
	assert.True(t, reportAudit.StrictGate.Enabled)
	assert.True(t, reportAudit.StrictGate.Pass)
	assert.Equal(t, "AuditMode", reportAudit.StrictGate.Reason)
}

// TestResourceVersionIsOpaque ensures resource versions are only compared as
// opaque strings and never used for numeric ordering across clusters.
func TestResourceVersionIsOpaque(t *testing.T) {
	assert.True(t, SameMemberResourceVersion("10", "10"))
	assert.False(t, SameMemberResourceVersion("9", "10"), "resource versions are not numbers")
	assert.False(t, SameMemberResourceVersion("", "1"))

	targets := []Target{
		coverageTarget("member-a", "uid-a", 1),
		coverageTarget("member-b", "uid-b", 1),
	}
	clusters := []clusterv1alpha1.Cluster{
		readyCluster("member-a", "uid-a"),
		readyCluster("member-b", "uid-b"),
	}
	// Deliberately give member-a a lexicographically "larger" RV. Output
	// ordering must remain cluster-name based, never RV based.
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "m-a", deploymentRaw(2, templateGeneration, 1, 1, 1, 1, 0)),
		deploymentEvidence("member-b", "uid-b", "m-b", deploymentRaw(2, templateGeneration, 1, 1, 1, 1, 0)),
	}
	evidences[0].Member.ResourceVersion = "9999"
	evidences[1].Member.ResourceVersion = "1"

	report, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)
	require.Len(t, report.Clusters, 2)
	assert.Equal(t, "member-a", report.Clusters[0].Cluster.Name)
	assert.Equal(t, "member-b", report.Clusters[1].Cluster.Name)
	assert.Equal(t, "9999", report.Clusters[0].MemberObject.ResourceVersion)
}

// TestBuildDoesNotMutateInputs proves the report is read-only with respect to
// the targets and evidences handed to it.
func TestBuildDoesNotMutateInputs(t *testing.T) {
	targets := []Target{coverageTarget("member-a", "uid-a", 3)}
	clusters := []clusterv1alpha1.Cluster{readyCluster("member-a", "uid-a")}
	evidences := []Evidence{
		deploymentEvidence("member-a", "uid-a", "member-uid-a",
			deploymentRaw(2, templateGeneration, 3, 3, 3, 3, 0)),
	}
	rawSnapshot := string(evidences[0].Item.StatusRaw)

	_, err := Build(deploymentInfo(), targets, clusters, evidences, Options{})
	require.NoError(t, err)
	assert.Equal(t, int32(3), targets[0].DesiredReplicas)
	assert.Equal(t, rawSnapshot, string(evidences[0].Item.StatusRaw))
	assert.Equal(t, metav1.ConditionTrue, clusters[0].Status.Conditions[0].Status)
}
