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

package affinity

import (
	"errors"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
)

// fixtureCluster builds a snapshot cluster with the given stable identity.
func fixtureCluster(uid, name string, clusterLabels map[string]string) *clusterv1alpha1.Cluster {
	return &clusterv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			UID:    types.UID(uid),
			Labels: mapsClone(clusterLabels),
		},
	}
}

func mapsClone(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func stringUIDs(uids ...string) []types.UID {
	out := make([]types.UID, 0, len(uids))
	for _, uid := range uids {
		out = append(out, types.UID(uid))
	}
	return out
}

func vocabOf(allowed map[string][]string) Vocabulary {
	return Vocabulary{AllowedValues: allowed}
}

// assertExact verifies the suggestion through the real ClusterMatches
// predicate and checks that no name-based selector fields are present.
func assertExact(t *testing.T, snapshot []*clusterv1alpha1.Cluster,
	targets []types.UID, res *SuggestClusterAffinityResult,
) {
	t.Helper()
	require.NotNil(t, res.Affinity)
	assert.Nil(t, res.Affinity.ClusterNames)
	assert.Nil(t, res.Affinity.ExcludeClusters)
	assert.Nil(t, res.Affinity.FieldSelector)

	expected := sets.List(sets.New(targets...))
	assert.Equal(t, expected, MatchedUIDs(snapshot, *res.Affinity))
	assert.Equal(t, expected, res.MatchedUIDs)
	assert.Len(t, res.Affinity.LabelSelector.MatchExpressions, res.RequirementCount)
}

// suggestStable runs the suggestion twice, once on a shuffled snapshot copy,
// and asserts deterministic, order-independent results.
func suggestStable(t *testing.T, req SuggestClusterAffinityRequest) *SuggestClusterAffinityResult {
	t.Helper()
	first, err := SuggestClusterAffinity(req)
	require.NoError(t, err)

	second, err := SuggestClusterAffinity(req)
	require.NoError(t, err)
	if diff := cmp.Diff(first, second); diff != "" {
		t.Fatalf("repeated suggestions differ (-first +second):\n%s", diff)
	}

	shuffled := slices.Clone(req.Snapshot)
	slices.Reverse(shuffled)
	shuffledReq := req
	shuffledReq.Snapshot = shuffled
	third, err := SuggestClusterAffinity(shuffledReq)
	require.NoError(t, err)
	assert.Equal(t, first.Snapshot.ID, third.Snapshot.ID)
	assert.Equal(t, first.Status, third.Status)
	if diff := cmp.Diff(first.Affinity, third.Affinity); diff != "" {
		t.Fatalf("suggestions differ under snapshot reordering (-first +third):\n%s", diff)
	}
	assert.Equal(t, first.MatchedUIDs, third.MatchedUIDs)
	return first
}

func singleRequirement(t *testing.T, res *SuggestClusterAffinityResult) metav1.LabelSelectorRequirement {
	t.Helper()
	require.NotNil(t, res.Affinity)
	require.Len(t, res.Affinity.LabelSelector.MatchExpressions, 1)
	return res.Affinity.LabelSelector.MatchExpressions[0]
}

func TestSuggestClusterAffinity_MissingLabelSemantics(t *testing.T) {
	t.Run("DoesNotExist selects clusters missing the key", func(t *testing.T) {
		snapshot := []*clusterv1alpha1.Cluster{
			fixtureCluster("uid-t1", "target", nil),
			fixtureCluster("uid-n1", "non-target", map[string]string{"env": "prod"}),
		}
		req := SuggestClusterAffinityRequest{
			Snapshot:   snapshot,
			TargetUIDs: stringUIDs("uid-t1"),
			Vocabulary: vocabOf(map[string][]string{"env": {"prod", "dev"}}),
		}
		res := suggestStable(t, req)
		require.Equal(t, StatusOptimal, res.Status)
		assert.Equal(t, 1, res.RequirementCount)

		req2 := singleRequirement(t, res)
		assert.Equal(t, "env", req2.Key)
		assert.Equal(t, metav1.LabelSelectorOpDoesNotExist, req2.Operator)
		assert.Empty(t, req2.Values)
		assertExact(t, snapshot, stringUIDs("uid-t1"), res)
	})

	t.Run("NotIn matches a missing key instead of treating it as empty string", func(t *testing.T) {
		// The target set contains one cluster missing "env" and one carrying
		// env=dev. Real NotIn semantics accept the missing key; an empty-string
		// equivalence would be unrepresentable and is never used.
		snapshot := []*clusterv1alpha1.Cluster{
			fixtureCluster("uid-t1", "target-missing", nil),
			fixtureCluster("uid-t2", "target-dev", map[string]string{"env": "dev"}),
			fixtureCluster("uid-n1", "non-target-prod", map[string]string{"env": "prod"}),
		}
		req := SuggestClusterAffinityRequest{
			Snapshot:   snapshot,
			TargetUIDs: stringUIDs("uid-t1", "uid-t2"),
			Vocabulary: vocabOf(map[string][]string{"env": {"prod", "dev"}}),
		}
		res := suggestStable(t, req)
		require.Equal(t, StatusOptimal, res.Status)
		assert.Equal(t, 1, res.RequirementCount)

		req2 := singleRequirement(t, res)
		assert.Equal(t, "env", req2.Key)
		assert.Equal(t, metav1.LabelSelectorOpNotIn, req2.Operator)
		assert.Equal(t, []string{"prod"}, req2.Values)
		assertExact(t, snapshot, stringUIDs("uid-t1", "uid-t2"), res)
	})

	t.Run("unmentionable values still allow separation through allowed values", func(t *testing.T) {
		snapshot := []*clusterv1alpha1.Cluster{
			fixtureCluster("uid-t1", "target", map[string]string{"env": "prod"}),
			fixtureCluster("uid-n1", "non-target", map[string]string{"env": "staging"}),
		}
		// "staging" is not in the closed vocabulary and must never be mentioned.
		req := SuggestClusterAffinityRequest{
			Snapshot:   snapshot,
			TargetUIDs: stringUIDs("uid-t1"),
			Vocabulary: vocabOf(map[string][]string{"env": {"prod"}}),
		}
		res := suggestStable(t, req)
		require.Equal(t, StatusOptimal, res.Status)
		req2 := singleRequirement(t, res)
		assert.Equal(t, metav1.LabelSelectorOpIn, req2.Operator)
		assert.Equal(t, []string{"prod"}, req2.Values)
		for _, expr := range res.Affinity.LabelSelector.MatchExpressions {
			assert.NotContains(t, expr.Values, "staging")
		}
		assertExact(t, snapshot, stringUIDs("uid-t1"), res)
	})
}

func TestSuggestClusterAffinity_SameNameRebuild(t *testing.T) {
	// Two clusters share a name: one was deleted and another re-created with
	// the same name. Only UIDs distinguish them, and the synthesized affinity
	// must not rely on or leak the shared name.
	snapshot := []*clusterv1alpha1.Cluster{
		fixtureCluster("uid-a", "member", map[string]string{"env": "prod", "tier": "fe"}),
		fixtureCluster("uid-b", "member", map[string]string{"env": "prod", "tier": "be"}),
	}
	base := SuggestClusterAffinityRequest{
		Snapshot:   snapshot,
		Vocabulary: vocabOf(map[string][]string{"env": {"prod"}, "tier": {"fe", "be"}}),
	}

	base.TargetUIDs = stringUIDs("uid-a")
	resA := suggestStable(t, base)
	require.Equal(t, StatusOptimal, resA.Status)
	reqA := singleRequirement(t, resA)
	assert.Equal(t, "tier", reqA.Key)
	assert.Equal(t, metav1.LabelSelectorOpIn, reqA.Operator)
	assert.Equal(t, []string{"fe"}, reqA.Values)
	assertExact(t, snapshot, stringUIDs("uid-a"), resA)
	assert.Nil(t, resA.Affinity.ClusterNames)
	assert.Nil(t, resA.Affinity.ExcludeClusters)

	base.TargetUIDs = stringUIDs("uid-b")
	resB := suggestStable(t, base)
	require.Equal(t, StatusOptimal, resB.Status)
	reqB := singleRequirement(t, resB)
	assert.Equal(t, []string{"be"}, reqB.Values)
	assertExact(t, snapshot, stringUIDs("uid-b"), resB)
}

func TestSuggestClusterAffinity_InseparableLabels(t *testing.T) {
	vocab := vocabOf(map[string][]string{"env": {"prod", "dev"}})

	t.Run("identical projections on the allowed vocabulary", func(t *testing.T) {
		// The clusters differ only on "secret", a key outside the vocabulary.
		snapshot := []*clusterv1alpha1.Cluster{
			fixtureCluster("uid-t", "target", map[string]string{"env": "prod", "secret": "x"}),
			fixtureCluster("uid-n", "non-target", map[string]string{"env": "prod", "secret": "y"}),
		}
		res := suggestStable(t, SuggestClusterAffinityRequest{
			Snapshot:   snapshot,
			TargetUIDs: stringUIDs("uid-t"),
			Vocabulary: vocab,
		})
		require.Equal(t, StatusInfeasible, res.Status)
		assert.Nil(t, res.Affinity)
		require.Len(t, res.Witnesses, 1)
		assert.Equal(t, IndistinguishablePair{TargetUID: "uid-t", NonTargetUID: "uid-n"}, res.Witnesses[0])
	})

	t.Run("fully identical labels", func(t *testing.T) {
		snapshot := []*clusterv1alpha1.Cluster{
			fixtureCluster("uid-t", "target", map[string]string{"env": "prod"}),
			fixtureCluster("uid-n", "non-target", map[string]string{"env": "prod"}),
		}
		res := suggestStable(t, SuggestClusterAffinityRequest{
			Snapshot:   snapshot,
			TargetUIDs: stringUIDs("uid-t"),
			Vocabulary: vocabOf(map[string][]string{"env": {"prod", "dev"}}),
		})
		require.Equal(t, StatusInfeasible, res.Status)
		require.Len(t, res.Witnesses, 1)
		assert.Equal(t, types.UID("uid-t"), res.Witnesses[0].TargetUID)
		assert.Equal(t, types.UID("uid-n"), res.Witnesses[0].NonTargetUID)
	})

	t.Run("unmentionable value differences collapse into one signature", func(t *testing.T) {
		snapshot := []*clusterv1alpha1.Cluster{
			fixtureCluster("uid-t", "target", map[string]string{"env": "prod", "note": "a"}),
			fixtureCluster("uid-n", "non-target", map[string]string{"env": "prod", "note": "b"}),
		}
		res := suggestStable(t, SuggestClusterAffinityRequest{
			Snapshot:   snapshot,
			TargetUIDs: stringUIDs("uid-t"),
			Vocabulary: vocabOf(map[string][]string{"env": {"prod"}}),
		})
		require.Equal(t, StatusInfeasible, res.Status)
		require.NotEmpty(t, res.Witnesses)
	})

	t.Run("vocabulary is never expanded from unrelated labels", func(t *testing.T) {
		snapshot := []*clusterv1alpha1.Cluster{
			fixtureCluster("uid-t", "target", map[string]string{"env": "prod", "owner": "alice"}),
			fixtureCluster("uid-n", "non-target", map[string]string{"env": "dev", "owner": "bob"}),
		}
		res := suggestStable(t, SuggestClusterAffinityRequest{
			Snapshot:   snapshot,
			TargetUIDs: stringUIDs("uid-t"),
			Vocabulary: vocab,
		})
		require.Equal(t, StatusOptimal, res.Status)
		for _, expr := range res.Affinity.LabelSelector.MatchExpressions {
			assert.NotEqual(t, "owner", expr.Key)
		}
		assertExact(t, snapshot, stringUIDs("uid-t"), res)
	})
}

func TestSuggestClusterAffinity_EquivalentOptimaDeterministic(t *testing.T) {
	// Two independent single requirements select exactly the target. The
	// canonical tie-break must always pick the same one.
	snapshot := []*clusterv1alpha1.Cluster{
		fixtureCluster("uid-t", "target", map[string]string{"a": "1", "b": "1"}),
		fixtureCluster("uid-n", "non-target", map[string]string{"a": "2", "b": "2"}),
	}
	req := SuggestClusterAffinityRequest{
		Snapshot:   snapshot,
		TargetUIDs: stringUIDs("uid-t"),
		Vocabulary: vocabOf(map[string][]string{
			"a": {"1", "2"},
			"b": {"1", "2"},
		}),
	}

	var first *SuggestClusterAffinityResult
	for i := 0; i < 20; i++ {
		res, err := SuggestClusterAffinity(req)
		require.NoError(t, err)
		require.Equal(t, StatusOptimal, res.Status)
		assert.Equal(t, 1, res.RequirementCount)
		if first == nil {
			first = res
			continue
		}
		if diff := cmp.Diff(first, res); diff != "" {
			t.Fatalf("equivalent-optima tie-break is non-deterministic (-first +got):\n%s", diff)
		}
	}
	require.NotNil(t, first)
	assert.Equal(t, "a", singleRequirement(t, first).Key)
	assertExact(t, snapshot, stringUIDs("uid-t"), first)
}

func TestSuggestClusterAffinity_MultiValueIn(t *testing.T) {
	snapshot := []*clusterv1alpha1.Cluster{
		fixtureCluster("uid-t1", "target-1", map[string]string{"env": "prod"}),
		fixtureCluster("uid-t2", "target-2", map[string]string{"env": "dev"}),
		fixtureCluster("uid-n1", "non-target", map[string]string{"env": "qa"}),
	}
	res := suggestStable(t, SuggestClusterAffinityRequest{
		Snapshot:   snapshot,
		TargetUIDs: stringUIDs("uid-t1", "uid-t2"),
		Vocabulary: vocabOf(map[string][]string{"env": {"prod", "dev", "qa"}}),
	})
	require.Equal(t, StatusOptimal, res.Status)
	req2 := singleRequirement(t, res)
	assert.Equal(t, metav1.LabelSelectorOpIn, req2.Operator)
	assert.Equal(t, []string{"dev", "prod"}, req2.Values)
	assertExact(t, snapshot, stringUIDs("uid-t1", "uid-t2"), res)
}

func TestSuggestClusterAffinity_AllTargetsEmptySelector(t *testing.T) {
	snapshot := []*clusterv1alpha1.Cluster{
		fixtureCluster("uid-c1", "c1", map[string]string{"env": "prod"}),
		fixtureCluster("uid-c2", "c2", map[string]string{"env": "dev"}),
		fixtureCluster("uid-c3", "c3", nil),
	}
	res := suggestStable(t, SuggestClusterAffinityRequest{
		Snapshot:   snapshot,
		TargetUIDs: stringUIDs("uid-c1", "uid-c2", "uid-c3"),
		Vocabulary: vocabOf(map[string][]string{"env": {"prod", "dev"}}),
	})
	require.Equal(t, StatusOptimal, res.Status)
	require.NotNil(t, res.Affinity)
	assert.Empty(t, res.Affinity.LabelSelector.MatchExpressions)
	assert.Equal(t, 0, res.RequirementCount)
	assert.Equal(t, 0, res.ProvenLowerBound)
	assertExact(t, snapshot, stringUIDs("uid-c1", "uid-c2", "uid-c3"), res)
}

// costTwoFixture returns a scenario whose proven minimum is two requirements.
func costTwoFixture() ([]*clusterv1alpha1.Cluster, Vocabulary) {
	snapshot := []*clusterv1alpha1.Cluster{
		fixtureCluster("uid-t1", "t1", map[string]string{"env": "prod", "tier": "fe"}),
		fixtureCluster("uid-t2", "t2", map[string]string{"env": "prod", "tier": "be"}),
		fixtureCluster("uid-n1", "n1", map[string]string{"env": "prod", "tier": "legacy"}),
		fixtureCluster("uid-n2", "n2", map[string]string{"env": "dev", "tier": "fe"}),
		fixtureCluster("uid-n3", "n3", map[string]string{"tier": "fe"}),
	}
	vocab := vocabOf(map[string][]string{
		"env":  {"prod", "dev"},
		"tier": {"fe", "be", "legacy"},
	})
	return snapshot, vocab
}

func TestSuggestClusterAffinity_BudgetReturnsBoundedIncomplete(t *testing.T) {
	snapshot, vocab := costTwoFixture()
	targets := stringUIDs("uid-t1", "uid-t2")

	t.Run("full budget proves the two-requirement optimum", func(t *testing.T) {
		res := suggestStable(t, SuggestClusterAffinityRequest{
			Snapshot:   snapshot,
			TargetUIDs: targets,
			Vocabulary: vocab,
		})
		require.Equal(t, StatusOptimal, res.Status)
		assert.False(t, res.BudgetExhausted)
		assert.Equal(t, 2, res.RequirementCount)
		assert.Equal(t, 2, res.ProvenLowerBound)
		assert.Equal(t, 2, res.ProvenUpperBound)
		assertExact(t, snapshot, targets, res)
	})

	t.Run("exhausted budget returns an exact but bounded result", func(t *testing.T) {
		res, err := SuggestClusterAffinity(SuggestClusterAffinityRequest{
			Snapshot:         snapshot,
			TargetUIDs:       targets,
			Vocabulary:       vocab,
			SearchNodeBudget: 1,
		})
		require.NoError(t, err)
		require.Equal(t, StatusIncomplete, res.Status)
		assert.True(t, res.BudgetExhausted)
		assert.GreaterOrEqual(t, res.NodesEvaluated, int64(1))
		if res.Affinity != nil {
			// A returned suggestion must remain exactly verified by the filter.
			assertExact(t, snapshot, targets, res)
			assert.GreaterOrEqual(t, res.ProvenUpperBound, res.ProvenLowerBound)
			assert.Equal(t, res.RequirementCount, res.ProvenUpperBound)
			assert.LessOrEqual(t, res.ProvenLowerBound, MaxRequirements)
		}
	})
}

func TestSuggestClusterAffinity_ItemCapInfeasible(t *testing.T) {
	snapshot, vocab := costTwoFixture()
	res, err := SuggestClusterAffinity(SuggestClusterAffinityRequest{
		Snapshot:        snapshot,
		TargetUIDs:      stringUIDs("uid-t1", "uid-t2"),
		Vocabulary:      vocab,
		MaxRequirements: 1,
	})
	require.NoError(t, err)
	require.Equal(t, StatusInfeasible, res.Status)
	assert.Nil(t, res.Affinity)
	assert.Empty(t, res.Witnesses)
	assert.Contains(t, res.Message, "maximum")
}

func TestSuggestClusterAffinity_CandidateCapIncomplete(t *testing.T) {
	// Nine distinct values create 510 retained requirements and exceed the
	// fixed candidate family cap, so an honest Incomplete answer is required.
	snapshot := make([]*clusterv1alpha1.Cluster, 0, 9)
	allowed := make([]string, 0, 9)
	for i := 0; i < 9; i++ {
		value := "v" + string(rune('0'+i))
		allowed = append(allowed, value)
		snapshot = append(snapshot, fixtureCluster("uid-"+value, "cluster-"+value,
			map[string]string{"k": value}))
	}
	res, err := SuggestClusterAffinity(SuggestClusterAffinityRequest{
		Snapshot:   snapshot,
		TargetUIDs: stringUIDs("uid-v0"),
		Vocabulary: vocabOf(map[string][]string{"k": allowed}),
	})
	require.NoError(t, err)
	require.Equal(t, StatusIncomplete, res.Status)
	assert.Nil(t, res.Affinity)
	assert.Contains(t, res.Message, "cap")
}

func TestSuggestClusterAffinity_ValidationErrors(t *testing.T) {
	validCluster := fixtureCluster("uid-1", "c1", map[string]string{"env": "prod"})
	validVocab := vocabOf(map[string][]string{"env": {"prod"}})

	tests := []struct {
		name string
		req  SuggestClusterAffinityRequest
	}{
		{
			name: "empty snapshot",
			req: SuggestClusterAffinityRequest{
				TargetUIDs: stringUIDs("uid-1"),
				Vocabulary: validVocab,
			},
		},
		{
			name: "nil snapshot entry",
			req: SuggestClusterAffinityRequest{
				Snapshot:   []*clusterv1alpha1.Cluster{nil},
				TargetUIDs: stringUIDs("uid-1"),
				Vocabulary: validVocab,
			},
		},
		{
			name: "empty cluster uid",
			req: SuggestClusterAffinityRequest{
				Snapshot:   []*clusterv1alpha1.Cluster{fixtureCluster("", "c1", nil)},
				TargetUIDs: stringUIDs("uid-1"),
				Vocabulary: validVocab,
			},
		},
		{
			name: "duplicate snapshot uid",
			req: SuggestClusterAffinityRequest{
				Snapshot: []*clusterv1alpha1.Cluster{
					fixtureCluster("uid-1", "c1", nil),
					fixtureCluster("uid-1", "c2", nil),
				},
				TargetUIDs: stringUIDs("uid-1"),
				Vocabulary: validVocab,
			},
		},
		{
			name: "empty target set",
			req: SuggestClusterAffinityRequest{
				Snapshot:   []*clusterv1alpha1.Cluster{validCluster},
				Vocabulary: validVocab,
			},
		},
		{
			name: "unknown target uid",
			req: SuggestClusterAffinityRequest{
				Snapshot:   []*clusterv1alpha1.Cluster{validCluster},
				TargetUIDs: stringUIDs("uid-unknown"),
				Vocabulary: validVocab,
			},
		},
		{
			name: "empty vocabulary",
			req: SuggestClusterAffinityRequest{
				Snapshot:   []*clusterv1alpha1.Cluster{validCluster},
				TargetUIDs: stringUIDs("uid-1"),
			},
		},
		{
			name: "invalid label key",
			req: SuggestClusterAffinityRequest{
				Snapshot:   []*clusterv1alpha1.Cluster{validCluster},
				TargetUIDs: stringUIDs("uid-1"),
				Vocabulary: vocabOf(map[string][]string{"bad key": {"prod"}}),
			},
		},
		{
			name: "invalid label value",
			req: SuggestClusterAffinityRequest{
				Snapshot:   []*clusterv1alpha1.Cluster{validCluster},
				TargetUIDs: stringUIDs("uid-1"),
				Vocabulary: vocabOf(map[string][]string{"env": {"bad value!"}}),
			},
		},
		{
			name: "max requirements above fixed grammar",
			req: SuggestClusterAffinityRequest{
				Snapshot:        []*clusterv1alpha1.Cluster{validCluster},
				TargetUIDs:      stringUIDs("uid-1"),
				Vocabulary:      validVocab,
				MaxRequirements: MaxRequirements + 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SuggestClusterAffinity(tt.req)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidRequest))
		})
	}
}

func TestSuggestClusterAffinity_SnapshotIdentity(t *testing.T) {
	snapshot := []*clusterv1alpha1.Cluster{
		fixtureCluster("uid-c1", "c1", nil),
		fixtureCluster("uid-c2", "c2", nil),
		fixtureCluster("uid-c3", "c3", nil),
	}
	vocab := vocabOf(map[string][]string{"env": {"prod"}})

	first, err := SuggestClusterAffinity(SuggestClusterAffinityRequest{
		Snapshot:   snapshot,
		TargetUIDs: stringUIDs("uid-c1"),
		Vocabulary: vocab,
	})
	require.NoError(t, err)

	reversed := slices.Clone(snapshot)
	slices.Reverse(reversed)
	second, err := SuggestClusterAffinity(SuggestClusterAffinityRequest{
		Snapshot:   reversed,
		TargetUIDs: stringUIDs("uid-c1"),
		Vocabulary: vocab,
	})
	require.NoError(t, err)

	assert.Equal(t, first.Snapshot.ID, second.Snapshot.ID)
	assert.Equal(t, 3, first.Snapshot.ClusterCount)
	assert.Equal(t, stringUIDs("uid-c1", "uid-c2", "uid-c3"), first.Snapshot.UIDs)
	assert.Len(t, first.Snapshot.ID, snapshotIDHexLength)

	changed, err := SuggestClusterAffinity(SuggestClusterAffinityRequest{
		Snapshot: []*clusterv1alpha1.Cluster{
			fixtureCluster("uid-c1", "c1", nil),
			fixtureCluster("uid-c2", "c2", nil),
			fixtureCluster("uid-other", "c3", nil),
		},
		TargetUIDs: stringUIDs("uid-c1"),
		Vocabulary: vocab,
	})
	require.NoError(t, err)
	assert.NotEqual(t, first.Snapshot.ID, changed.Snapshot.ID)
}

// TestSuggestClusterAffinity_ExhaustiveOracle enumerates every conjunction up
// to a fixed depth over a small vocabulary as an independent reference, and
// checks every non-empty target subset of the fixture.
func TestSuggestClusterAffinity_ExhaustiveOracle(t *testing.T) {
	snapshot := []*clusterv1alpha1.Cluster{
		fixtureCluster("uid-c1", "c1", map[string]string{"env": "prod", "zone": "z1"}),
		fixtureCluster("uid-c2", "c2", map[string]string{"env": "prod", "zone": "z2"}),
		fixtureCluster("uid-c3", "c3", map[string]string{"env": "dev", "zone": "z1"}),
		fixtureCluster("uid-c4", "c4", map[string]string{"env": "dev"}),
		fixtureCluster("uid-c5", "c5", nil),
	}
	allowed := map[string][]string{
		"env":  {"prod", "dev"},
		"zone": {"z1", "z2"},
	}
	family := oracleRequirementFamily(allowed)
	matchMatrix := buildMatchMatrix(t, family, snapshot)

	for mask := 1; mask < (1 << len(snapshot)); mask++ {
		targets := make([]types.UID, 0, len(snapshot))
		for bit := range snapshot {
			if mask&(1<<bit) != 0 {
				targets = append(targets, snapshot[bit].UID)
			}
		}
		name := "targets=" + joinUIDs(targets)
		t.Run(name, func(t *testing.T) {
			res, err := SuggestClusterAffinity(SuggestClusterAffinityRequest{
				Snapshot:   snapshot,
				TargetUIDs: targets,
				Vocabulary: vocabOf(allowed),
			})
			require.NoError(t, err)

			targetSet := sets.New(targets...)
			oracleFeasible, minCost, minExpressions := oracleSolve(family, matchMatrix, snapshot, targetSet)
			if oracleFeasible {
				require.Equal(t, StatusOptimal, res.Status)
				assertExact(t, snapshot, targets, res)
				assert.Equal(t, minCost, res.RequirementCount)
				assert.Contains(t, minExpressions, testCanonicalSet(res.Affinity.LabelSelector.MatchExpressions))
			} else {
				require.Equal(t, StatusInfeasible, res.Status)
				assert.Nil(t, res.Affinity)
				expectedWitnesses := oracleWitnesses(family, matchMatrix, snapshot, targetSet)
				assert.Equal(t, expectedWitnesses, witnessSet(res.Witnesses))
			}
		})
	}
}

// oracleRequirementFamily independently enumerates all single requirements
// allowed by the small vocabulary.
func oracleRequirementFamily(allowed map[string][]string) []metav1.LabelSelectorRequirement {
	keys := make([]string, 0, len(allowed))
	for key := range allowed {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	var family []metav1.LabelSelectorRequirement
	for _, key := range keys {
		values := slices.Clone(allowed[key])
		slices.Sort(values)
		family = append(family,
			metav1.LabelSelectorRequirement{Key: key, Operator: metav1.LabelSelectorOpExists},
			metav1.LabelSelectorRequirement{Key: key, Operator: metav1.LabelSelectorOpDoesNotExist},
		)
		for subMask := 1; subMask < (1 << len(values)); subMask++ {
			subset := make([]string, 0, len(values))
			for bit, value := range values {
				if subMask&(1<<bit) != 0 {
					subset = append(subset, value)
				}
			}
			family = append(family,
				metav1.LabelSelectorRequirement{Key: key, Operator: metav1.LabelSelectorOpIn, Values: subset},
				metav1.LabelSelectorRequirement{Key: key, Operator: metav1.LabelSelectorOpNotIn, Values: slices.Clone(subset)},
			)
		}
	}
	return family
}

// buildMatchMatrix precomputes requirement/cluster match outcomes using the
// real label selector conversion.
func buildMatchMatrix(t *testing.T, family []metav1.LabelSelectorRequirement,
	snapshot []*clusterv1alpha1.Cluster,
) [][]bool {
	t.Helper()
	matrix := make([][]bool, len(family))
	for i, expr := range family {
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{expr},
		})
		require.NoError(t, err)
		matrix[i] = make([]bool, len(snapshot))
		for j, cluster := range snapshot {
			matrix[i][j] = selector.Matches(labels.Set(cluster.GetLabels()))
		}
	}
	return matrix
}

// oracleSolve brute-forces the minimum conjunction over the requirement
// family and returns every minimum expression as canonical string sets.
func oracleSolve(family []metav1.LabelSelectorRequirement, matrix [][]bool,
	snapshot []*clusterv1alpha1.Cluster, targets sets.Set[types.UID],
) (bool, int, []string) {
	if targets.Len() == len(snapshot) {
		// The zero-requirement empty selector matches the whole snapshot.
		return true, 0, []string{testCanonicalSet(nil)}
	}

	preserving := oraclePreserving(family, matrix, snapshot, targets)
	if !allNonTargetsKillable(preserving, matrix, snapshot, targets) {
		return false, 0, nil
	}
	return oracleMinimumCover(family, preserving, matrix, snapshot, targets)
}

// oraclePreserving returns the family indices whose requirement matches every
// target cluster.
func oraclePreserving(family []metav1.LabelSelectorRequirement, matrix [][]bool,
	snapshot []*clusterv1alpha1.Cluster, targets sets.Set[types.UID],
) []int {
	preserving := make([]int, 0, len(family))
	for reqIdx := range family {
		matchesAllTargets := true
		for j, cluster := range snapshot {
			if targets.Has(cluster.UID) && !matrix[reqIdx][j] {
				matchesAllTargets = false
				break
			}
		}
		if matchesAllTargets {
			preserving = append(preserving, reqIdx)
		}
	}
	return preserving
}

// allNonTargetsKillable reports whether each non-target cluster fails at
// least one target-preserving requirement.
func allNonTargetsKillable(preserving []int, matrix [][]bool,
	snapshot []*clusterv1alpha1.Cluster, targets sets.Set[types.UID],
) bool {
	for _, cluster := range snapshot {
		if targets.Has(cluster.UID) {
			continue
		}
		position := indexOfUID(snapshot, cluster.UID)
		killable := false
		for _, reqIdx := range preserving {
			if !matrix[reqIdx][position] {
				killable = true
				break
			}
		}
		if !killable {
			return false
		}
	}
	return true
}

// oracleMinimumCover enumerates conjunctions by increasing size and returns
// the first exact size together with every exact expression at that size.
// Two keys cannot require more than four useful requirements on this fixture.
func oracleMinimumCover(family []metav1.LabelSelectorRequirement, preserving []int,
	matrix [][]bool, snapshot []*clusterv1alpha1.Cluster, targets sets.Set[types.UID],
) (bool, int, []string) {
	const maxDepth = 4
	for size := 1; size <= maxDepth && size <= len(preserving); size++ {
		var minimum []string
		for _, combo := range combinations(len(preserving), size) {
			if oracleComboExact(combo, preserving, matrix, snapshot, targets) {
				exprs := make([]metav1.LabelSelectorRequirement, 0, size)
				for _, pick := range combo {
					exprs = append(exprs, family[preserving[pick]])
				}
				minimum = append(minimum, testCanonicalSet(exprs))
			}
		}
		if len(minimum) > 0 {
			return true, size, minimum
		}
	}
	return false, 0, nil
}

// oracleComboExact evaluates one conjunction against the whole snapshot.
func oracleComboExact(combo, preserving []int, matrix [][]bool,
	snapshot []*clusterv1alpha1.Cluster, targets sets.Set[types.UID],
) bool {
	for j, cluster := range snapshot {
		matched := true
		for _, pick := range combo {
			if !matrix[preserving[pick]][j] {
				matched = false
				break
			}
		}
		if matched != targets.Has(cluster.UID) {
			return false
		}
	}
	return true
}

// oracleWitnesses computes indistinguishable pairs from full requirement
// outcome vectors, independently of the production signature logic.
func oracleWitnesses(family []metav1.LabelSelectorRequirement, matrix [][]bool,
	snapshot []*clusterv1alpha1.Cluster, targets sets.Set[types.UID],
) map[IndistinguishablePair]struct{} {
	expected := make(map[IndistinguishablePair]struct{})
	for i, ci := range snapshot {
		if !targets.Has(ci.UID) {
			continue
		}
		for j, cj := range snapshot {
			if targets.Has(cj.UID) {
				continue
			}
			equivalent := true
			for reqIdx := range family {
				if matrix[reqIdx][i] != matrix[reqIdx][j] {
					equivalent = false
					break
				}
			}
			if equivalent {
				expected[IndistinguishablePair{TargetUID: ci.UID, NonTargetUID: cj.UID}] = struct{}{}
			}
		}
	}
	return expected
}

func witnessSet(witnesses []IndistinguishablePair) map[IndistinguishablePair]struct{} {
	out := make(map[IndistinguishablePair]struct{}, len(witnesses))
	for _, pair := range witnesses {
		out[pair] = struct{}{}
	}
	return out
}

func indexOfUID(snapshot []*clusterv1alpha1.Cluster, uid types.UID) int {
	for i, cluster := range snapshot {
		if cluster.UID == uid {
			return i
		}
	}
	return -1
}

// combinations returns every index combination of the given size drawn from
// n elements.
func combinations(n, size int) [][]int {
	var out [][]int
	current := make([]int, size)
	var walk func(start, depth int)
	walk = func(start, depth int) {
		if depth == size {
			out = append(out, slices.Clone(current))
			return
		}
		for i := start; i <= n-(size-depth); i++ {
			current[depth] = i
			walk(i+1, depth+1)
		}
	}
	walk(0, 0)
	return out
}

// testCanonicalSet renders expressions as the canonical set key used by the
// oracle comparison.
func testCanonicalSet(exprs []metav1.LabelSelectorRequirement) string {
	parts := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		values := slices.Clone(expr.Values)
		slices.Sort(values)
		parts = append(parts, expr.Key+"/"+string(expr.Operator)+"/"+joinStrings(values))
	}
	slices.Sort(parts)
	return joinStrings(parts)
}

func joinStrings(values []string) string {
	out := ""
	for i, value := range values {
		if i > 0 {
			out += ","
		}
		out += value
	}
	return out
}

func joinUIDs(uids []types.UID) string {
	parts := make([]string, 0, len(uids))
	for _, uid := range uids {
		parts = append(parts, string(uid))
	}
	slices.Sort(parts)
	return joinStrings(parts)
}
