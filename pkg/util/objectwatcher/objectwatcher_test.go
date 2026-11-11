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

package objectwatcher

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/default/native"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/default/native/prune"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
)

const (
	testClusterName = "cluster"
	testNamespace   = "default"
	testPodName     = "test"
	testWorkNS      = "karmada-es-cluster"
	testWorkName    = "work"
)

func podGVR() schema.GroupVersionResource {
	return corev1.SchemeGroupVersion.WithResource("pods")
}

func configMapGVR() schema.GroupVersionResource {
	return corev1.SchemeGroupVersion.WithResource("configmaps")
}

func desiredPod(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testPodName,
			Labels: map[string]string{
				util.ManagedByKarmadaLabel: util.ManagedByKarmadaLabelValue,
				"app":                      "nginx",
			},
			Annotations: map[string]string{
				workv1alpha2.WorkNamespaceAnnotation: testWorkNS,
				workv1alpha2.WorkNameAnnotation:      testWorkName,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "nginx", Image: "nginx:1.19.0"}},
		},
	}
	raw, err := json.Marshal(pod)
	require.NoError(t, err)
	u := &unstructured.Unstructured{}
	require.NoError(t, u.UnmarshalJSON(raw))
	// Mimic the Work mutating webhook, which prunes server-managed fields before the
	// manifest is stored and dispatched, so desired objects never carry status/uid/resourceVersion.
	require.NoError(t, prune.RemoveIrrelevantFields(u, prune.RemoveJobTTLSeconds))
	return u
}

func managedClusterPod(t *testing.T, uid types.UID, rv string) *unstructured.Unstructured {
	u := desiredPod(t)
	u.SetUID(uid)
	u.SetResourceVersion(rv)
	return u
}

// ConfigMaps have no retain interpreter hook, which makes no-op Update tests deterministic.
func desiredConfigMap(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testPodName,
			Labels: map[string]string{
				util.ManagedByKarmadaLabel: util.ManagedByKarmadaLabelValue,
				"app":                      "nginx",
			},
			Annotations: map[string]string{
				workv1alpha2.WorkNamespaceAnnotation: testWorkNS,
				workv1alpha2.WorkNameAnnotation:      testWorkName,
			},
		},
		Data: map[string]string{"key": "value"},
	}
	raw, err := json.Marshal(cm)
	require.NoError(t, err)
	u := &unstructured.Unstructured{}
	require.NoError(t, u.UnmarshalJSON(raw))
	require.NoError(t, prune.RemoveIrrelevantFields(u, prune.RemoveJobTTLSeconds))
	return u
}

func managedClusterConfigMap(t *testing.T, uid types.UID, rv string) *unstructured.Unstructured {
	u := desiredConfigMap(t)
	u.SetUID(uid)
	u.SetResourceVersion(rv)
	return u
}

func setReceiptsGate(t *testing.T, enabled bool) {
	t.Helper()
	original := features.FeatureGate.DeepCopy()
	t.Cleanup(func() { features.FeatureGate = original })
	require.NoError(t, features.FeatureGate.Set(fmt.Sprintf("%s=%t", features.WorkApplyReceipts, enabled)))
}

type watcherFixture struct {
	watcher       ObjectWatcher
	dynamicClient *dynamicfake.FakeDynamicClient
	manager       genericmanager.MultiClusterInformerManager
	restMapper    meta.RESTMapper
}

type fixtureOption struct {
	// keepCacheEmpty forces the informer LIST to return no objects and swallows watch
	// events, so GetObjectFromCache deterministically reports NotFound regardless of tracker state.
	keepCacheEmpty bool
}

func newWatcherFixture(t *testing.T, seedObjects ...runtime.Object) *watcherFixture {
	return newWatcherFixtureWithOption(t, fixtureOption{}, seedObjects...)
}

func newEmptyCacheWatcherFixture(t *testing.T) *watcherFixture {
	return newWatcherFixtureWithOption(t, fixtureOption{keepCacheEmpty: true})
}

func newWatcherFixtureWithOption(t *testing.T, option fixtureOption, seedObjects ...runtime.Object) *watcherFixture {
	t.Helper()
	restMapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	restMapper.Add(corev1.SchemeGroupVersion.WithKind("Pod"), meta.RESTScopeNamespace)
	restMapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)

	dynamicClient := dynamicfake.NewSimpleDynamicClient(scheme.Scheme, seedObjects...)
	if option.keepCacheEmpty {
		for _, resource := range []string{"pods", "configmaps"} {
			dynamicClient.PrependReactor("list", resource, func(clienttesting.Action) (bool, runtime.Object, error) {
				emptyList := &unstructured.UnstructuredList{}
				emptyList.SetResourceVersion("0")
				return true, emptyList, nil
			})
			dynamicClient.PrependWatchReactor(resource, func(clienttesting.Action) (bool, watch.Interface, error) {
				return true, watch.NewFake(), nil
			})
		}
	}
	clusterClientSetFunc := func(string, client.Client, *util.ClientOption) (*util.DynamicClusterClient, error) {
		return &util.DynamicClusterClient{
			ClusterName:      testClusterName,
			DynamicClientSet: dynamicClient,
		}, nil
	}
	manager := genericmanager.NewMultiClusterInformerManager(context.Background(), fedinformer.StripUnusedFields)
	manager.ForCluster(testClusterName, dynamicClient, 0).Lister(podGVR())
	manager.ForCluster(testClusterName, dynamicClient, 0).Lister(configMapGVR())
	manager.Start(testClusterName)
	manager.WaitForCacheSync(testClusterName)

	watcher := NewObjectWatcher(nil, restMapper, clusterClientSetFunc, nil,
		&testResourceInterpreter{DefaultInterpreter: native.NewDefaultInterpreter()}, manager)
	return &watcherFixture{
		watcher:       watcher,
		dynamicClient: dynamicClient,
		manager:       manager,
		restMapper:    restMapper,
	}
}

func actionCount(actions []clienttesting.Action, verb, resource string) int {
	count := 0
	for _, action := range actions {
		if action.GetVerb() == verb && action.GetResource().Resource == resource {
			count++
		}
	}
	return count
}

// testResourceInterpreter completes the ResourceInterpreter interface for tests.
type testResourceInterpreter struct {
	*native.DefaultInterpreter
}

func (t *testResourceInterpreter) Start(context.Context) error { return nil }

func TestCreateReturnsRealMemberObjectOnCacheMiss(t *testing.T) {
	setReceiptsGate(t, true)
	fixture := newWatcherFixture(t)
	fixture.dynamicClient.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		created := action.(clienttesting.CreateAction).GetObject().(*unstructured.Unstructured)
		created.SetUID("server-uid-1")
		created.SetResourceVersion("100")
		return false, nil, nil
	})

	outcome, err := fixture.watcher.Create(context.Background(), testClusterName, desiredPod(t))
	require.NoError(t, err)
	require.NotNil(t, outcome)
	assert.Equal(t, OperationResultCreated, outcome.Operation)
	assert.False(t, outcome.Readback)
	require.NotNil(t, outcome.Object)
	assert.Equal(t, types.UID("server-uid-1"), outcome.Object.GetUID())
	assert.Equal(t, "100", outcome.Object.GetResourceVersion())

	// The receipt evidence must exactly match the object the member API returns on direct GET.
	direct, err := fixture.dynamicClient.Resource(podGVR()).Namespace(testNamespace).
		Get(context.Background(), testPodName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, direct.GetUID(), outcome.Object.GetUID())
	assert.Equal(t, direct.GetResourceVersion(), outcome.Object.GetResourceVersion())
	assert.Equal(t, 1, actionCount(fixture.dynamicClient.Actions(), "create", "pods"))
}

func TestCreateCacheHitReadbackUnderGate(t *testing.T) {
	setReceiptsGate(t, true)
	seed := managedClusterPod(t, "cache-uid", "cached-rv")
	fixture := newWatcherFixture(t, seed)

	outcome, err := fixture.watcher.Create(context.Background(), testClusterName, desiredPod(t))
	require.NoError(t, err)
	require.NotNil(t, outcome)
	assert.Equal(t, OperationResultObserved, outcome.Operation)
	assert.True(t, outcome.Readback)
	require.NotNil(t, outcome.Object)
	assert.Equal(t, types.UID("cache-uid"), outcome.Object.GetUID())
	assert.Equal(t, "cached-rv", outcome.Object.GetResourceVersion())
	// A cache hit must never be turned into a create.
	assert.Equal(t, 0, actionCount(fixture.dynamicClient.Actions(), "create", "pods"))
	assert.GreaterOrEqual(t, actionCount(fixture.dynamicClient.Actions(), "get", "pods"), 1)
}

func TestCreateCacheHitLegacyBehaviorWhenGateOff(t *testing.T) {
	setReceiptsGate(t, false)
	seed := managedClusterPod(t, "cache-uid", "cached-rv")
	fixture := newWatcherFixture(t, seed)

	outcome, err := fixture.watcher.Create(context.Background(), testClusterName, desiredPod(t))
	require.NoError(t, err)
	require.NotNil(t, outcome)
	assert.Equal(t, OperationResultNone, outcome.Operation)
	assert.Nil(t, outcome.Object)
	assert.Equal(t, 0, actionCount(fixture.dynamicClient.Actions(), "create", "pods"))
	assert.Equal(t, 0, actionCount(fixture.dynamicClient.Actions(), "get", "pods"))
}

func TestCreateAlreadyExistsResolvedByReadback(t *testing.T) {
	setReceiptsGate(t, true)
	// Cache starts empty; the member API already owns the object (stale informer scenario).
	fixture := newEmptyCacheWatcherFixture(t)
	require.NoError(t, fixture.dynamicClient.Tracker().Create(podGVR(), managedClusterPod(t, "existing-uid", "7"), testNamespace))

	outcome, err := fixture.watcher.Create(context.Background(), testClusterName, desiredPod(t))
	require.NoError(t, err)
	require.NotNil(t, outcome)
	assert.True(t, outcome.Readback)
	assert.Equal(t, types.UID("existing-uid"), outcome.Object.GetUID())
	assert.Equal(t, "7", outcome.Object.GetResourceVersion())
	assert.Equal(t, 1, actionCount(fixture.dynamicClient.Actions(), "create", "pods"))
}

func TestCreateConflictOnForeignObject(t *testing.T) {
	setReceiptsGate(t, true)
	foreign := managedClusterPod(t, "foreign-uid", "3")
	foreign.SetLabels(map[string]string{"someone-else": "true"})
	fixture := newWatcherFixture(t, foreign)

	outcome, err := fixture.watcher.Create(context.Background(), testClusterName, desiredPod(t))
	require.Error(t, err)
	assert.True(t, stderrors.Is(err, ErrResourceConflict))
	assert.Nil(t, outcome)

	// The foreign object must not be modified and must not gain the Karmada managed label.
	direct, getErr := fixture.dynamicClient.Resource(podGVR()).Namespace(testNamespace).
		Get(context.Background(), testPodName, metav1.GetOptions{})
	require.NoError(t, getErr)
	assert.Equal(t, "true", direct.GetLabels()["someone-else"])
	assert.NotEqual(t, util.ManagedByKarmadaLabelValue, direct.GetLabels()[util.ManagedByKarmadaLabel])
	assert.Equal(t, "3", direct.GetResourceVersion())
}

func TestCreateAmbiguousErrorConfirmedByReadback(t *testing.T) {
	setReceiptsGate(t, true)
	fixture := newEmptyCacheWatcherFixture(t)
	require.NoError(t, fixture.dynamicClient.Tracker().Create(podGVR(), managedClusterPod(t, "landed-uid", "9"), testNamespace))

	createCalls := 0
	fixture.dynamicClient.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		createCalls++
		return true, nil, apierrors.NewTimeoutError("request did not complete within requested duration", 1)
	})

	outcome, err := fixture.watcher.Create(context.Background(), testClusterName, desiredPod(t))
	require.NoError(t, err, "a verified readback must resolve the ambiguous create without error")
	require.NotNil(t, outcome)
	assert.True(t, outcome.Readback)
	assert.Equal(t, OperationResultObserved, outcome.Operation)
	assert.Equal(t, types.UID("landed-uid"), outcome.Object.GetUID())
	assert.Equal(t, 1, createCalls, "the ambiguous create must not be retried blindly")
}

func TestCreateAmbiguousErrorWithoutLandedObjectReturnsError(t *testing.T) {
	setReceiptsGate(t, true)
	fixture := newEmptyCacheWatcherFixture(t)
	fixture.dynamicClient.PrependReactor("create", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewTimeoutError("request did not complete within requested duration", 1)
	})

	outcome, err := fixture.watcher.Create(context.Background(), testClusterName, desiredPod(t))
	require.Error(t, err)
	assert.Nil(t, outcome)
	assert.True(t, apierrors.IsTimeout(err))
}

func TestUpdateReturnsRealResponseAndReadbackGuardsStaleCache(t *testing.T) {
	setReceiptsGate(t, true)

	t.Run("changed update returns write response object", func(t *testing.T) {
		existing := managedClusterConfigMap(t, "uid-1", "1")
		fixture := newWatcherFixture(t, existing)
		fixture.dynamicClient.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
			updated := action.(clienttesting.UpdateAction).GetObject().(*unstructured.Unstructured)
			updated.SetUID("uid-1")
			updated.SetResourceVersion("2")
			return false, nil, nil
		})

		desired := desiredConfigMap(t)
		require.NoError(t, unstructured.SetNestedField(desired.Object, "changed", "data", "key"))
		outcome, err := fixture.watcher.Update(context.Background(), testClusterName, desired, existing)
		require.NoError(t, err)
		require.NotNil(t, outcome)
		assert.Equal(t, OperationResultUpdated, outcome.Operation)
		assert.False(t, outcome.Readback)
		assert.Equal(t, "2", outcome.Object.GetResourceVersion())
		assert.Equal(t, types.UID("uid-1"), outcome.Object.GetUID())
	})

	t.Run("unchanged update uses direct readback instead of stale cache RV", func(t *testing.T) {
		existing := managedClusterConfigMap(t, "uid-2", "cached-1")
		fixture := newWatcherFixture(t, existing)
		fixture.dynamicClient.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
			getAction := action.(clienttesting.GetAction)
			obj, err := fixture.dynamicClient.Tracker().Get(getAction.GetResource(), getAction.GetNamespace(), getAction.GetName())
			require.NoError(t, err)
			server := obj.(*unstructured.Unstructured).DeepCopy()
			server.SetResourceVersion("server-fresh-9")
			return true, server, nil
		})

		outcome, err := fixture.watcher.Update(context.Background(), testClusterName, desiredConfigMap(t), existing)
		require.NoError(t, err)
		require.NotNil(t, outcome)
		assert.True(t, outcome.Readback)
		assert.Equal(t, OperationResultObserved, outcome.Operation)
		assert.Equal(t, "server-fresh-9", outcome.Object.GetResourceVersion(), "stale cache RV must never be fabricated into the outcome")
		assert.Equal(t, 0, actionCount(fixture.dynamicClient.Actions(), "update", "configmaps"))
	})

	t.Run("gate off unchanged update preserves legacy cache behavior", func(t *testing.T) {
		setReceiptsGate(t, false)
		existing := managedClusterConfigMap(t, "uid-3", "cached-2")
		fixture := newWatcherFixture(t, existing)

		outcome, err := fixture.watcher.Update(context.Background(), testClusterName, desiredConfigMap(t), existing)
		require.NoError(t, err)
		require.NotNil(t, outcome)
		assert.Equal(t, OperationResultNone, outcome.Operation)
		assert.False(t, outcome.Readback)
		assert.Equal(t, "cached-2", outcome.Object.GetResourceVersion())
		assert.Equal(t, 0, actionCount(fixture.dynamicClient.Actions(), "get", "configmaps"))
	})
}

func TestUpdateAmbiguousErrorConfirmedByReadback(t *testing.T) {
	setReceiptsGate(t, true)
	existing := managedClusterConfigMap(t, "uid-4", "1")
	fixture := newWatcherFixture(t, existing)
	fixture.dynamicClient.PrependReactor("update", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewTimeoutError("update timed out", 1)
	})

	outcome, err := fixture.watcher.Update(context.Background(), testClusterName, desiredConfigMap(t), existing)
	require.NoError(t, err)
	require.NotNil(t, outcome)
	assert.True(t, outcome.Readback)
	assert.Equal(t, OperationResultObserved, outcome.Operation)
	assert.Equal(t, "1", outcome.Object.GetResourceVersion())
}
