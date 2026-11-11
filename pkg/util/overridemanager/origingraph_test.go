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

package overridemanager

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-openapi/jsonpointer"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/util/gclient"
	utilhelper "github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/test/helper"
)

const (
	testOriginCluster   = "test1"
	testOriginNamespace = metav1.NamespaceDefault
	testOriginResource  = "test1"
)

func setOriginGraphGate(t *testing.T, enabled bool) {
	t.Helper()
	previous := features.FeatureGate.Enabled(features.OverrideOriginGraph)
	if err := features.FeatureGate.Set(fmt.Sprintf("%s=%t", features.OverrideOriginGraph, enabled)); err != nil {
		t.Fatalf("failed to set OverrideOriginGraph gate to %t: %v", enabled, err)
	}
	t.Cleanup(func() {
		_ = features.FeatureGate.Set(fmt.Sprintf("%s=%t", features.OverrideOriginGraph, previous))
	})
}

func newOriginTestManager(t *testing.T, objs ...client.Object) *overrideManagerImpl {
	t.Helper()
	return &overrideManagerImpl{
		Client:        fake.NewClientBuilder().WithScheme(gclient.NewSchema()).WithObjects(objs...).Build(),
		EventRecorder: record.NewFakeRecorder(1024),
	}
}

func newOriginTestCluster() *clusterv1alpha1.Cluster {
	cluster := helper.NewCluster(testOriginCluster)
	cluster.UID = types.UID("cluster-uid-1")
	return cluster
}

func deploymentSelector() []policyv1alpha1.ResourceSelector {
	return resourceSelector("apps/v1", "Deployment")
}

func resourceSelector(apiVersion, kind string) []policyv1alpha1.ResourceSelector {
	return []policyv1alpha1.ResourceSelector{{
		APIVersion: apiVersion,
		Kind:       kind,
		Namespace:  testOriginNamespace,
		Name:       testOriginResource,
	}}
}

// pointerString reads a string field by RFC6901 pointer, tolerating list indices.
func pointerString(t *testing.T, obj *unstructured.Unstructured, path string) string {
	t.Helper()
	pointer, err := jsonpointer.New(path)
	if err != nil {
		t.Fatalf("invalid pointer %s: %v", path, err)
	}
	res, kind, err := pointer.Get(obj.Object)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	if kind != reflect.String {
		t.Fatalf("field %s is not a string (kind=%v)", path, kind)
	}
	return res.(string)
}

// pointerStringSlice reads a string list field by RFC6901 pointer, tolerating list indices.
func pointerStringSlice(t *testing.T, obj *unstructured.Unstructured, path string) []string {
	t.Helper()
	pointer, err := jsonpointer.New(path)
	if err != nil {
		t.Fatalf("invalid pointer %s: %v", path, err)
	}
	res, _, err := pointer.Get(obj.Object)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	raw, ok := res.([]any)
	if !ok {
		t.Fatalf("field %s is not a list: %T", path, res)
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		out = append(out, fmt.Sprintf("%v", item))
	}
	return out
}

func matchingClusterRule(overriders policyv1alpha1.Overriders) []policyv1alpha1.RuleWithCluster {
	return []policyv1alpha1.RuleWithCluster{{
		TargetCluster: &policyv1alpha1.ClusterAffinity{ClusterNames: []string{testOriginCluster}},
		Overriders:    overriders,
	}}
}

func newClusterOverridePolicyWithMeta(name, uid string, generation int64, overriders policyv1alpha1.Overriders) *policyv1alpha1.ClusterOverridePolicy {
	return &policyv1alpha1.ClusterOverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid), Generation: generation},
		Spec: policyv1alpha1.OverrideSpec{
			ResourceSelectors: deploymentSelector(),
			OverrideRules:     matchingClusterRule(overriders),
		},
	}
}

func newOverridePolicyWithMeta(name, uid string, generation int64, overriders policyv1alpha1.Overriders) *policyv1alpha1.OverridePolicy {
	return newOverridePolicyWithSelector(name, uid, generation, deploymentSelector(), overriders)
}

func newOverridePolicyWithSelector(name, uid string, generation int64, selectors []policyv1alpha1.ResourceSelector, overriders policyv1alpha1.Overriders) *policyv1alpha1.OverridePolicy {
	return &policyv1alpha1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testOriginNamespace, UID: types.UID(uid), Generation: generation},
		Spec: policyv1alpha1.OverrideSpec{
			ResourceSelectors: selectors,
			OverrideRules:     matchingClusterRule(overriders),
		},
	}
}

func findOrigin(graph *OverrideOriginGraph, path string) *FieldOrigin {
	for i := range graph.Fields {
		if graph.Fields[i].Path == path {
			return &graph.Fields[i]
		}
	}
	return nil
}

// requireFinalEvidence proves that the final value at path is backed by a real active write record
// attributed to the named policy, rather than guessed from the policy name.
func requireFinalEvidence(t *testing.T, graph *OverrideOriginGraph, path, policyName string) FieldWriteRecord {
	t.Helper()
	origin := findOrigin(graph, path)
	if origin == nil {
		t.Fatalf("expected origin node for %s, got fields: %v", path, graph.FieldPaths())
	}
	if origin.FinalOwner == nil {
		t.Fatalf("field %s has no final owner; writes=%+v", path, origin.Writes)
	}
	for _, write := range origin.Writes {
		if write.Sequence == *origin.FinalOwner {
			if write.State != writeStateActive {
				t.Fatalf("final owner of %s is not active: %+v", path, write)
			}
			if write.Policy.Name != policyName {
				t.Fatalf("final owner of %s is policy %s, want %s", path, write.Policy.Name, policyName)
			}
			return write
		}
	}
	t.Fatalf("final owner seq %d of %s has no matching write record", *origin.FinalOwner, path)
	return FieldWriteRecord{}
}

// FieldPaths is a test-friendly dump of tracked paths.
func (g *OverrideOriginGraph) FieldPaths() []string {
	paths := make([]string, 0, len(g.Fields))
	for _, field := range g.Fields {
		paths = append(paths, field.Path)
	}
	return paths
}

func TestOverrideOriginGraph_GateDisabledReturnsNilGraph(t *testing.T) {
	setOriginGraphGate(t, false)
	deployment, err := utilhelper.ToUnstructured(helper.NewDeployment(testOriginNamespace, testOriginResource))
	if err != nil {
		t.Fatal(err)
	}
	policy := newOverridePolicyWithMeta("op-a", "uid-op-a", 1, policyv1alpha1.Overriders{
		Plaintext: []policyv1alpha1.PlaintextOverrider{{
			Path:     "/spec/replicas",
			Operator: policyv1alpha1.OverriderOpReplace,
			Value:    apiextensionsv1.JSON{Raw: []byte(`5`)},
		}},
	})
	manager := newOriginTestManager(t, newOriginTestCluster(), policy)

	cops, ops, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(deployment, testOriginCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if graph != nil {
		t.Fatalf("expected nil graph when gate is disabled, got %+v", graph)
	}
	if ops == nil || len(ops.AppliedItems) != 1 {
		t.Fatalf("legacy AppliedOverrides must be preserved, got %+v", ops)
	}
	if cops != nil {
		t.Fatalf("expected no cluster overrides, got %+v", cops)
	}
}

func TestOverrideOriginGraph_SameFieldMultiplePolicies(t *testing.T) {
	setOriginGraphGate(t, true)
	deployment, err := utilhelper.ToUnstructured(helper.NewDeployment(testOriginNamespace, testOriginResource))
	if err != nil {
		t.Fatal(err)
	}
	first := newClusterOverridePolicyWithMeta("cop-a", "uid-cop-a", 2, policyv1alpha1.Overriders{
		Plaintext: []policyv1alpha1.PlaintextOverrider{{
			Path:     "/spec/replicas",
			Operator: policyv1alpha1.OverriderOpReplace,
			Value:    apiextensionsv1.JSON{Raw: []byte(`5`)},
		}},
	})
	second := newClusterOverridePolicyWithMeta("cop-b", "uid-cop-b", 7, policyv1alpha1.Overriders{
		Plaintext: []policyv1alpha1.PlaintextOverrider{{
			Path:     "/spec/replicas",
			Operator: policyv1alpha1.OverriderOpReplace,
			Value:    apiextensionsv1.JSON{Raw: []byte(`7`)},
		}},
	})
	manager := newOriginTestManager(t, newOriginTestCluster(), first, second)

	cops, ops, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(deployment, testOriginCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cops == nil || len(cops.AppliedItems) != 2 {
		t.Fatalf("AppliedOverrides list must be preserved, got %+v", cops)
	}
	if ops != nil {
		t.Fatalf("expected no namespaced overrides, got %+v", ops)
	}

	replicas, ok, err := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
	if err != nil || !ok || replicas != 7 {
		t.Fatalf("final replicas = %d (ok=%v), want 7", replicas, ok)
	}

	origin := findOrigin(graph, "/spec/replicas")
	if origin == nil || len(origin.Writes) != 2 {
		t.Fatalf("expected two real writes on /spec/replicas, got %+v", origin)
	}
	if origin.Writes[0].State != writeStateSuperseded || origin.Writes[1].State != writeStateActive {
		t.Fatalf("expected superseded->active states, got %+v", origin.Writes)
	}
	if origin.Writes[0].SupersededBy == nil || *origin.Writes[0].SupersededBy != origin.Writes[1].Sequence {
		t.Fatalf("first write must point at the second write sequence, got %+v", origin.Writes)
	}
	finalWrite := requireFinalEvidence(t, graph, "/spec/replicas", "cop-b")
	if finalWrite.Value != float64(7) {
		t.Fatalf("final write value = %v, want 7", finalWrite.Value)
	}
	if finalWrite.Policy.UID != "uid-cop-b" || finalWrite.Policy.Generation != 7 {
		t.Fatalf("final write must bind policy UID/generation, got %+v", finalWrite.Policy)
	}

	// Graph bindings: policy revisions, cluster UID and template digest.
	if graph.Cluster.UID != "cluster-uid-1" || graph.TemplateDigest == "" {
		t.Fatalf("graph must bind cluster UID and template digest, got %+v", graph)
	}
	wantPolicies := map[string]PolicyRef{}
	for _, ref := range graph.Policies {
		wantPolicies[ref.Name] = ref
	}
	if wantPolicies["cop-a"].UID != "uid-cop-a" || wantPolicies["cop-a"].Generation != 2 ||
		wantPolicies["cop-a"].Kind != policyv1alpha1.ResourceKindClusterOverridePolicy {
		t.Fatalf("policy bindings missing revision identity: %+v", wantPolicies)
	}
}

func TestOverrideOriginGraph_ClusterThenNamespacedOverlay(t *testing.T) {
	setOriginGraphGate(t, true)
	deployment, err := utilhelper.ToUnstructured(helper.NewDeployment(testOriginNamespace, testOriginResource))
	if err != nil {
		t.Fatal(err)
	}
	clusterPolicy := newClusterOverridePolicyWithMeta("cop-a", "uid-cop-a", 1, policyv1alpha1.Overriders{
		LabelsOverrider: []policyv1alpha1.LabelAnnotationOverrider{{
			Operator: policyv1alpha1.OverriderOpAdd,
			Value:    map[string]string{"owner": "cluster-team"},
		}},
	})
	namespacedPolicy := newOverridePolicyWithMeta("op-a", "uid-op-a", 1, policyv1alpha1.Overriders{
		LabelsOverrider: []policyv1alpha1.LabelAnnotationOverrider{{
			Operator: policyv1alpha1.OverriderOpReplace,
			Value:    map[string]string{"owner": "namespace-team"},
		}},
	})
	manager := newOriginTestManager(t, newOriginTestCluster(), clusterPolicy, namespacedPolicy)

	cops, ops, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(deployment, testOriginCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cops == nil || ops == nil {
		t.Fatalf("both AppliedOverrides lists must be preserved, cops=%+v ops=%+v", cops, ops)
	}

	owner := pointerString(t, deployment, "/metadata/labels/owner")
	if owner != "namespace-team" {
		t.Fatalf("final label owner = %q, want namespace-team", owner)
	}

	origin := findOrigin(graph, "/metadata/labels/owner")
	if origin == nil || len(origin.Writes) != 2 {
		t.Fatalf("expected cluster then namespaced writes, got %+v", origin)
	}
	if origin.Writes[0].Policy.Kind != policyv1alpha1.ResourceKindClusterOverridePolicy ||
		origin.Writes[1].Policy.Kind != policyv1alpha1.ResourceKindOverridePolicy {
		t.Fatalf("apply order must be ClusterOverridePolicy then OverridePolicy, got %+v", origin.Writes)
	}
	if origin.Writes[0].State != writeStateSuperseded {
		t.Fatalf("cluster policy write must be superseded, got %+v", origin.Writes[0])
	}
	requireFinalEvidence(t, graph, "/metadata/labels/owner", "op-a")
}

func TestOverrideOriginGraph_ImagePartialRewriteAndContainerIdentity(t *testing.T) {
	setOriginGraphGate(t, true)
	deployment, err := utilhelper.ToUnstructured(helper.NewDeployment(testOriginNamespace, testOriginResource))
	if err != nil {
		t.Fatal(err)
	}
	registryPolicy := newClusterOverridePolicyWithMeta("cop-registry", "uid-cop-registry", 1, policyv1alpha1.Overriders{
		ImageOverrider: []policyv1alpha1.ImageOverrider{{
			Component: policyv1alpha1.Registry,
			Operator:  policyv1alpha1.OverriderOpReplace,
			Value:     "my.registry.example",
		}},
	})
	tagPolicy := newOverridePolicyWithMeta("op-tag", "uid-op-tag", 1, policyv1alpha1.Overriders{
		ImageOverrider: []policyv1alpha1.ImageOverrider{{
			Component: policyv1alpha1.Tag,
			Operator:  policyv1alpha1.OverriderOpReplace,
			Value:     "v2",
		}},
	})
	commandPolicy := newOverridePolicyWithMeta("op-command", "uid-op-command", 1, policyv1alpha1.Overriders{
		CommandOverrider: []policyv1alpha1.CommandArgsOverrider{{
			ContainerName: "nginx",
			Operator:      policyv1alpha1.OverriderOpAdd,
			Value:         []string{"--debug"},
		}},
	})
	manager := newOriginTestManager(t, newOriginTestCluster(), registryPolicy, tagPolicy, commandPolicy)

	_, _, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(deployment, testOriginCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	imagePath := "/spec/template/spec/containers/0/image"
	finalImage := pointerString(t, deployment, imagePath)
	if finalImage != "my.registry.example/nginx:v2" {
		t.Fatalf("final image = %q, want my.registry.example/nginx:v2", finalImage)
	}

	origin := findOrigin(graph, imagePath)
	if origin == nil || len(origin.Writes) != 2 {
		t.Fatalf("expected registry and tag writes at container image path, got %+v", origin)
	}
	for _, write := range origin.Writes {
		if write.Container == nil || write.Container.Type != "containers" || write.Container.Index != 0 || write.Container.Name != "nginx" {
			t.Fatalf("image write must carry real container identity, got %+v", write.Container)
		}
	}
	if origin.Writes[0].ImageComponent != string(policyv1alpha1.Registry) ||
		origin.Writes[1].ImageComponent != string(policyv1alpha1.Tag) {
		t.Fatalf("image split components not traced, got %+v", origin.Writes)
	}
	finalImageWrite := requireFinalEvidence(t, graph, imagePath, "op-tag")
	if finalImageWrite.Value != finalImage {
		t.Fatalf("traced value %v differs from actual final image %q", finalImageWrite.Value, finalImage)
	}

	commandPath := "/spec/template/spec/containers/0/command"
	command := pointerStringSlice(t, deployment, commandPath)
	if len(command) != 1 || command[0] != "--debug" {
		t.Fatalf("final command = %v, want [--debug]", command)
	}
	commandWrite := requireFinalEvidence(t, graph, commandPath, "op-command")
	if commandWrite.Container == nil || commandWrite.Container.Name != "nginx" || commandWrite.Container.Index != 0 {
		t.Fatalf("command write must carry container identity, got %+v", commandWrite.Container)
	}
}

func TestOverrideOriginGraph_ArrayIndexMovement(t *testing.T) {
	setOriginGraphGate(t, true)
	deploy := helper.NewDeployment(testOriginNamespace, testOriginResource)
	deploy.Spec.Template.Spec.Containers[0].Command = []string{"x"}
	deployment, err := utilhelper.ToUnstructured(deploy)
	if err != nil {
		t.Fatal(err)
	}
	const commandPath = "/spec/template/spec/containers/0/command"
	firstPolicy := newClusterOverridePolicyWithMeta("cop-first", "uid-cop-first", 1, policyv1alpha1.Overriders{
		Plaintext: []policyv1alpha1.PlaintextOverrider{{
			Path:     commandPath + "/0",
			Operator: policyv1alpha1.OverriderOpAdd,
			Value:    apiextensionsv1.JSON{Raw: []byte(`"first"`)},
		}},
	})
	zeroPolicy := newOverridePolicyWithMeta("op-a-zero", "uid-op-zero", 1, policyv1alpha1.Overriders{
		Plaintext: []policyv1alpha1.PlaintextOverrider{{
			Path:     commandPath + "/0",
			Operator: policyv1alpha1.OverriderOpAdd,
			Value:    apiextensionsv1.JSON{Raw: []byte(`"zero"`)},
		}},
	})
	removePolicy := newOverridePolicyWithMeta("op-b-remove", "uid-op-remove", 1, policyv1alpha1.Overriders{
		Plaintext: []policyv1alpha1.PlaintextOverrider{{
			Path:     commandPath + "/0",
			Operator: policyv1alpha1.OverriderOpRemove,
		}},
	})
	manager := newOriginTestManager(t, newOriginTestCluster(), firstPolicy, zeroPolicy, removePolicy)

	_, _, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(deployment, testOriginCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	command := pointerStringSlice(t, deployment, commandPath)
	if len(command) != 2 || command[0] != "first" || command[1] != "x" {
		t.Fatalf("final command = %v, want [first x]", command)
	}

	// "first" was written at index 0 by cop-first, shifted to 1 by op-zero and back to 0 when
	// op-remove deleted index 0. Its final owner must remain the original write evidence.
	movedOrigin := findOrigin(graph, commandPath+"/0")
	if movedOrigin == nil {
		t.Fatalf("expected origin to follow real index movement back to %s/0, paths=%v", commandPath, graph.FieldPaths())
	}
	if movedOrigin.Removed {
		t.Fatalf("the surviving moved field must not be marked removed: %+v", movedOrigin)
	}
	if len(movedOrigin.Writes) != 1 || movedOrigin.Writes[0].Policy.Name != "cop-first" || movedOrigin.Writes[0].Value != "first" {
		t.Fatalf("moved field must keep its original write evidence, got %+v", movedOrigin.Writes)
	}
	requireFinalEvidence(t, graph, commandPath+"/0", "cop-first")

	// The "zero" element was deleted: its tombstone node must carry deletion evidence, keep the
	// last occupied path and have no final owner.
	var zeroOrigin *FieldOrigin
	for i := range graph.Fields {
		for _, write := range graph.Fields[i].Writes {
			if write.Policy.Name == "op-a-zero" && write.Op == string(policyv1alpha1.OverriderOpAdd) {
				zeroOrigin = &graph.Fields[i]
			}
		}
	}
	if zeroOrigin == nil {
		t.Fatalf("missing tombstone origin for deleted zero element")
	}
	if !zeroOrigin.Removed || zeroOrigin.FinalOwner != nil {
		t.Fatalf("deleted element must be a removed node without final owner, got %+v", zeroOrigin)
	}
	lastWrite := zeroOrigin.Writes[len(zeroOrigin.Writes)-1]
	if lastWrite.State != writeStateDeleted || lastWrite.Policy.Name != "op-b-remove" {
		t.Fatalf("expected deletion evidence from op-b-remove, got %+v", lastWrite)
	}

	// Real index movements must be observable on the operation log in both directions.
	var sawUpShift, sawDownShift bool
	for _, op := range graph.Operations {
		for _, shift := range op.ShiftedFields {
			if shift.FromIndex == 0 && shift.ToIndex == 1 {
				sawUpShift = true
			}
			if shift.FromIndex == 1 && shift.ToIndex == 0 {
				sawDownShift = true
			}
		}
	}
	if !sawUpShift || !sawDownShift {
		t.Fatalf("expected both up and down array shifts, operations=%+v", graph.Operations)
	}
}

func TestOverrideOriginGraph_FailureInterruptsAndNoFakeGraph(t *testing.T) {
	t.Run("plaintext replace on missing path keeps error protocol and discards graph", func(t *testing.T) {
		setOriginGraphGate(t, true)
		deployment, err := utilhelper.ToUnstructured(helper.NewDeployment(testOriginNamespace, testOriginResource))
		if err != nil {
			t.Fatal(err)
		}
		failing := newClusterOverridePolicyWithMeta("cop-fail", "uid-cop-fail", 1, policyv1alpha1.Overriders{
			Plaintext: []policyv1alpha1.PlaintextOverrider{{
				Path:     "/spec/nonexistent/leaf",
				Operator: policyv1alpha1.OverriderOpReplace,
				Value:    apiextensionsv1.JSON{Raw: []byte(`"x"`)},
			}},
		})
		manager := newOriginTestManager(t, newOriginTestCluster(), failing)

		cops, ops, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(deployment, testOriginCluster)
		if err == nil {
			t.Fatal("expected the original jsonpatch error for a missing path")
		}
		if graph != nil || cops != nil || ops != nil {
			t.Fatalf("a failed run must not emit a fake complete graph, got graph=%+v cops=%+v ops=%+v", graph, cops, ops)
		}
	})

	t.Run("field overrider on non-string field keeps legacy error", func(t *testing.T) {
		setOriginGraphGate(t, true)
		deployment, err := utilhelper.ToUnstructured(helper.NewDeployment(testOriginNamespace, testOriginResource))
		if err != nil {
			t.Fatal(err)
		}
		failing := newOverridePolicyWithMeta("op-fail", "uid-op-fail", 1, policyv1alpha1.Overriders{
			FieldOverrider: []policyv1alpha1.FieldOverrider{{
				FieldPath: "/spec/replicas",
				JSON: []policyv1alpha1.JSONPatchOperation{{
					SubPath:  "/x",
					Operator: policyv1alpha1.OverriderOpReplace,
					Value:    apiextensionsv1.JSON{Raw: []byte(`1`)},
				}},
			}},
		})
		manager := newOriginTestManager(t, newOriginTestCluster(), failing)

		_, _, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(deployment, testOriginCluster)
		if err == nil {
			t.Fatal("expected the legacy non-string field error")
		}
		if graph != nil {
			t.Fatalf("failed field overrider must not produce a graph, got %+v", graph)
		}
	})

	t.Run("move/copy/test operations are rejected before any write", func(t *testing.T) {
		if err := validateTracedOp("move", "/spec/replicas"); err == nil {
			t.Fatal("move operation must be rejected")
		}
		if err := validateTracedOp("copy", "/spec/replicas"); err == nil {
			t.Fatal("copy operation must be rejected")
		}
		if err := validateTracedOp("test", "/spec/replicas"); err == nil {
			t.Fatal("test operation must be rejected")
		}
	})
}

func TestOverrideOriginGraph_EquivalentToLegacyAndPristineNotPolluted(t *testing.T) {
	setOriginGraphGate(t, true)
	base := helper.NewDeployment(testOriginNamespace, testOriginResource)
	legacyObj, err := utilhelper.ToUnstructured(base)
	if err != nil {
		t.Fatal(err)
	}
	tracedObj := legacyObj.DeepCopy()

	overriders := policyv1alpha1.Overriders{
		ImageOverrider: []policyv1alpha1.ImageOverrider{{
			Component: policyv1alpha1.Registry,
			Operator:  policyv1alpha1.OverriderOpReplace,
			Value:     "my.registry/",
		}},
		LabelsOverrider: []policyv1alpha1.LabelAnnotationOverrider{{
			Operator: policyv1alpha1.OverriderOpAdd,
			Value:    map[string]string{"team": "platform"},
		}},
		Plaintext: []policyv1alpha1.PlaintextOverrider{{
			Path:     "/spec/replicas",
			Operator: policyv1alpha1.OverriderOpReplace,
			Value:    apiextensionsv1.JSON{Raw: []byte(`9`)},
		}},
	}
	policy := newClusterOverridePolicyWithMeta("cop-mix", "uid-cop-mix", 4, overriders)

	legacyManager := newOriginTestManager(t, newOriginTestCluster(), policy)
	legacyCops, legacyOps, legacyErr := legacyManager.ApplyOverridePolicies(legacyObj, testOriginCluster)
	if legacyErr != nil {
		t.Fatalf("legacy apply failed: %v", legacyErr)
	}
	tracedManager := newOriginTestManager(t, newOriginTestCluster(), policy)
	tracedCops, tracedOps, graph, tracedErr := tracedManager.ApplyOverridePoliciesWithOriginGraph(tracedObj, testOriginCluster)
	if tracedErr != nil {
		t.Fatalf("traced apply failed: %v", tracedErr)
	}

	legacyJSON, err := legacyObj.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	tracedJSON, err := tracedObj.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(legacyJSON, tracedJSON) {
		t.Fatalf("traced final object %s differs from legacy result %s", tracedJSON, legacyJSON)
	}
	legacyCopsJSON, _ := legacyCops.MarshalJSON()
	tracedCopsJSON, _ := tracedCops.MarshalJSON()
	if !bytes.Equal(legacyCopsJSON, tracedCopsJSON) {
		t.Fatalf("AppliedOverrides differ: legacy=%s traced=%s", legacyCopsJSON, tracedCopsJSON)
	}
	if legacyOps != nil || tracedOps != nil {
		t.Fatalf("unexpected namespaced overrides")
	}
	if graph == nil {
		t.Fatal("expected an origin graph when gate is enabled")
	}

	// The pristine input snapshot must never be polluted by the traced mutations.
	pristineInput, err := utilhelper.ToUnstructured(base)
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, err := pristineInput.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	target := pristineInput.DeepCopy()
	tracer := newOriginTracer(target, newOriginTestCluster(), pristineInput)
	if err := tracer.applyPolicyOverridersWithTracing(target, overriders, PolicyRef{
		Kind: policyv1alpha1.ResourceKindClusterOverridePolicy,
		Name: "cop-mix",
		UID:  "uid-cop-mix",
	}); err != nil {
		t.Fatalf("direct tracing failed: %v", err)
	}
	afterBytes, err := pristineInput.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeBytes, afterBytes) {
		t.Fatal("pristine input copy was polluted by traced overrides")
	}
}

func TestOverrideOriginGraph_SensitiveFieldsKeepPathAndSummaryOnly(t *testing.T) {
	setOriginGraphGate(t, true)
	secret, err := utilhelper.ToUnstructured(helper.NewSecret(testOriginNamespace, testOriginResource, map[string][]byte{
		"password": []byte("topsecret"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	policy := newOverridePolicyWithSelector("op-secret", "uid-op-secret", 1, resourceSelector("v1", "Secret"), policyv1alpha1.Overriders{
		Plaintext: []policyv1alpha1.PlaintextOverrider{
			{
				Path:     "/data/password",
				Operator: policyv1alpha1.OverriderOpReplace,
				Value:    apiextensionsv1.JSON{Raw: []byte(`"bmV3cGFzcw=="`)},
			},
		},
		LabelsOverrider: []policyv1alpha1.LabelAnnotationOverrider{{
			Operator: policyv1alpha1.OverriderOpAdd,
			Value:    map[string]string{"safe": "visible"},
		}},
	})
	manager := newOriginTestManager(t, newOriginTestCluster(), policy)

	_, _, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(secret, testOriginCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sensitiveWrite := requireFinalEvidence(t, graph, "/data/password", "op-secret")
	if !sensitiveWrite.Sensitive || sensitiveWrite.Value != nil || sensitiveWrite.ValueSummary == "" {
		t.Fatalf("sensitive write must keep summary only, got %+v", sensitiveWrite)
	}
	visibleWrite := requireFinalEvidence(t, graph, "/metadata/labels/safe", "op-secret")
	if visibleWrite.Sensitive || visibleWrite.Value != "visible" {
		t.Fatalf("non-sensitive write should retain its value, got %+v", visibleWrite)
	}
	raw, err := jsonMarshalGraph(graph)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("topsecret")) || bytes.Contains(raw, []byte("bmV3cGFzcw==")) {
		t.Fatalf("marshaled graph leaked sensitive value: %s", raw)
	}
	if !bytes.Contains(raw, []byte("/data/password")) {
		t.Fatalf("graph must retain the sensitive field path")
	}
}

func TestOverrideOriginGraph_YAMLFragmentsAreInputLevelWithoutLineNumbers(t *testing.T) {
	setOriginGraphGate(t, true)
	configMap, err := utilhelper.ToUnstructured(helper.NewConfigMap(testOriginNamespace, "test1", map[string]string{
		"test.yaml": "key:\n  key1: value\n",
	}))
	if err != nil {
		t.Fatal(err)
	}
	policy := newOverridePolicyWithSelector("op-yaml", "uid-op-yaml", 1, resourceSelector("v1", "ConfigMap"), policyv1alpha1.Overriders{
		FieldOverrider: []policyv1alpha1.FieldOverrider{{
			FieldPath: "/data/test.yaml",
			YAML: []policyv1alpha1.YAMLPatchOperation{{
				SubPath:  "/key/key1",
				Operator: policyv1alpha1.OverriderOpReplace,
				Value:    apiextensionsv1.JSON{Raw: []byte(`"updated_value"`)},
			}},
		}},
	})
	manager := newOriginTestManager(t, newOriginTestCluster(), policy)

	_, _, graph, err := manager.ApplyOverridePoliciesWithOriginGraph(configMap, testOriginCluster)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	compositePath := "/data/test.yaml#/key/key1"
	origin := findOrigin(graph, compositePath)
	if origin == nil {
		t.Fatalf("expected embedded YAML fragment origin at %s, paths=%v", compositePath, graph.FieldPaths())
	}
	if !origin.ExistedInInput || origin.InputSourceLevel != SourceLevelInputLevel {
		t.Fatalf("unknown YAML fragment must be marked InputLevel, got %+v", origin)
	}
	if origin.EmbeddedFieldPath != "/data/test.yaml" || origin.EmbeddedSubPath != "/key/key1" {
		t.Fatalf("embedded identity missing, got %+v", origin)
	}
	requireFinalEvidence(t, graph, compositePath, "op-yaml")

	data, ok, _ := unstructured.NestedStringMap(configMap.Object, "data")
	if !ok || data["test.yaml"] != "key:\n  key1: updated_value\n" {
		t.Fatalf("YAML override result mismatch: %v", data)
	}
	raw, err := jsonMarshalGraph(graph)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"line"`)) {
		t.Fatalf("YAML provenance must not fabricate source line numbers: %s", raw)
	}
}

func TestOverrideOriginGraph_LimitsAreEnforcedAndMarked(t *testing.T) {
	setOriginGraphGate(t, true)
	deployment, err := utilhelper.ToUnstructured(helper.NewDeployment(testOriginNamespace, testOriginResource))
	if err != nil {
		t.Fatal(err)
	}
	overriders := policyv1alpha1.Overriders{
		LabelsOverrider: []policyv1alpha1.LabelAnnotationOverrider{{
			Operator: policyv1alpha1.OverriderOpAdd,
			Value:    map[string]string{"a": "1", "b": "2", "c": "3"},
		}},
	}
	pristine := deployment.DeepCopy()
	tracer := newOriginTracer(deployment, newOriginTestCluster(), pristine)
	tightLimits := GraphLimits{MaxFields: 1, MaxOperations: 1, MaxOutputBytes: 900}
	tracer.limits = tightLimits
	tracer.graph.Limits = tightLimits

	if err := tracer.applyPolicyOverridersWithTracing(deployment, overriders, PolicyRef{
		Kind: policyv1alpha1.ResourceKindOverridePolicy,
		Name: "op-limited",
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	graph := tracer.finalize()
	if !graph.Truncated {
		t.Fatal("graph must be marked truncated when budgets are hit")
	}
	hasFieldReason, hasOperationReason := false, false
	for _, reason := range graph.TruncationReasons {
		if reason == truncationReasonFields {
			hasFieldReason = true
		}
		if reason == truncationReasonOperations {
			hasOperationReason = true
		}
	}
	if !hasFieldReason || !hasOperationReason {
		t.Fatalf("missing truncation reasons: %v", graph.TruncationReasons)
	}
	// Patches still all applied even though recording was capped.
	labels, _, _ := unstructured.NestedStringMap(deployment.Object, "metadata", "labels")
	if labels["a"] != "1" || labels["b"] != "2" || labels["c"] != "3" {
		t.Fatalf("limits must not change override outcome, labels=%v", labels)
	}
	// MarshalJSON trimming must not mutate the in-memory graph.
	var originalValue any
	if origin := findOrigin(graph, "/metadata/labels/a"); origin != nil && origin.FinalOwner != nil {
		for _, write := range origin.Writes {
			if write.Sequence == *origin.FinalOwner {
				originalValue = write.Value
			}
		}
	}
	raw, err := jsonMarshalGraph(graph)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 900 {
		t.Fatalf("marshaled graph %d bytes exceeds output budget", len(raw))
	}
	if origin := findOrigin(graph, "/metadata/labels/a"); origin != nil && origin.FinalOwner != nil {
		for _, write := range origin.Writes {
			if write.Sequence == *origin.FinalOwner && !reflect.DeepEqual(write.Value, originalValue) {
				t.Fatalf("MarshalJSON mutated in-memory graph value: got %v want %v", write.Value, originalValue)
			}
		}
	}
}

// jsonMarshalGraph avoids importing encoding/json in every test case above.
func jsonMarshalGraph(graph *OverrideOriginGraph) ([]byte, error) {
	return graph.MarshalJSON()
}
