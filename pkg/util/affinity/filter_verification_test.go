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

package affinity_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/scheduler/framework"
	"github.com/karmada-io/karmada/pkg/scheduler/framework/plugins/clusteraffinity"
	"github.com/karmada-io/karmada/pkg/util/affinity"
)

func filterCluster(t *testing.T, plugin framework.FilterPlugin,
	affinitySpec *policyv1alpha1.ClusterAffinity, cluster *clusterv1alpha1.Cluster,
) framework.Code {
	t.Helper()
	spec := &workv1alpha2.ResourceBindingSpec{
		Placement: &policyv1alpha1.Placement{
			ClusterAffinity: affinitySpec,
		},
	}
	return plugin.Filter(context.Background(), spec, nil, cluster).Code()
}

func runPluginFilter(t *testing.T, snapshot []*clusterv1alpha1.Cluster,
	res *affinity.SuggestClusterAffinityResult,
) sets.Set[types.UID] {
	t.Helper()
	plugin, err := clusteraffinity.New()
	require.NoError(t, err)
	filterPlugin, ok := plugin.(framework.FilterPlugin)
	require.True(t, ok)

	selected := sets.New[types.UID]()
	for _, cluster := range snapshot {
		if code := filterCluster(t, filterPlugin, res.Affinity, cluster); code == framework.Success {
			selected.Insert(cluster.UID)
		} else {
			assert.Equal(t, framework.Unschedulable, code)
		}
	}
	return selected
}

func TestSuggestedAffinityVerifiedBySchedulerFilter(t *testing.T) {
	tests := []struct {
		name     string
		snapshot []*clusterv1alpha1.Cluster
		targets  []types.UID
		vocab    affinity.Vocabulary
	}{
		{
			name: "missing-label DoesNotExist through the real filter",
			snapshot: []*clusterv1alpha1.Cluster{
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", UID: "uid-t1"}},
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{
					Name: "c2", UID: "uid-n1", Labels: map[string]string{"env": "prod"},
				}},
			},
			targets: []types.UID{"uid-t1"},
			vocab:   affinity.Vocabulary{AllowedValues: map[string][]string{"env": {"prod", "dev"}}},
		},
		{
			name: "multi-requirement conjunction through the real filter",
			snapshot: []*clusterv1alpha1.Cluster{
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{
					Name: "t1", UID: "uid-t1", Labels: map[string]string{"env": "prod", "tier": "fe"},
				}},
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{
					Name: "t2", UID: "uid-t2", Labels: map[string]string{"env": "prod", "tier": "be"},
				}},
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{
					Name: "n1", UID: "uid-n1", Labels: map[string]string{"env": "prod", "tier": "legacy"},
				}},
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{
					Name: "n2", UID: "uid-n2", Labels: map[string]string{"env": "dev", "tier": "fe"},
				}},
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{
					Name: "n3", UID: "uid-n3", Labels: map[string]string{"tier": "fe"},
				}},
			},
			targets: []types.UID{"uid-t1", "uid-t2"},
			vocab: affinity.Vocabulary{AllowedValues: map[string][]string{
				"env":  {"prod", "dev"},
				"tier": {"fe", "be", "legacy"},
			}},
		},
		{
			name: "empty selector selects every cluster through the real filter",
			snapshot: []*clusterv1alpha1.Cluster{
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", UID: "uid-c1", Labels: map[string]string{"env": "prod"}}},
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "c2", UID: "uid-c2", Labels: map[string]string{"env": "dev"}}},
				&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "c3", UID: "uid-c3"}},
			},
			targets: []types.UID{"uid-c1", "uid-c2", "uid-c3"},
			vocab:   affinity.Vocabulary{AllowedValues: map[string][]string{"env": {"prod", "dev"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := affinity.SuggestClusterAffinity(affinity.SuggestClusterAffinityRequest{
				Snapshot:   tt.snapshot,
				TargetUIDs: tt.targets,
				Vocabulary: tt.vocab,
			})
			require.NoError(t, err)
			require.NotNil(t, res.Affinity)
			require.Equal(t, affinity.StatusOptimal, res.Status)

			selected := runPluginFilter(t, tt.snapshot, res)
			assert.Equal(t, sets.New(tt.targets...), selected)
			assert.Equal(t, sets.List(sets.New(tt.targets...)), res.MatchedUIDs)
		})
	}
}

func TestInfeasibleSuggestionSkipsFilter(t *testing.T) {
	snapshot := []*clusterv1alpha1.Cluster{
		&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{
			Name: "target", UID: "uid-t", Labels: map[string]string{"env": "prod"},
		}},
		&clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{
			Name: "non-target", UID: "uid-n", Labels: map[string]string{"env": "prod"},
		}},
	}
	res, err := affinity.SuggestClusterAffinity(affinity.SuggestClusterAffinityRequest{
		Snapshot:   snapshot,
		TargetUIDs: []types.UID{"uid-t"},
		Vocabulary: affinity.Vocabulary{AllowedValues: map[string][]string{"env": {"prod", "dev"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, affinity.StatusInfeasible, res.Status)
	assert.Nil(t, res.Affinity)
	require.Len(t, res.Witnesses, 1)
	assert.Equal(t, types.UID("uid-t"), res.Witnesses[0].TargetUID)
	assert.Equal(t, types.UID("uid-n"), res.Witnesses[0].NonTargetUID)
}
