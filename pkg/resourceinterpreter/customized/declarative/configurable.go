/*
Copyright 2022 The Karmada Authors.

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

package declarative

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/customized/declarative/configmanager"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/customized/declarative/luavm"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/customized/declarative/shadow"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"github.com/karmada-io/karmada/pkg/util/interpreter/validation"
)

// ShadowRunner is the optional side-by-side evaluator for local Lua customizations.
// Implementations must be fully non-blocking and observational: they never return
// candidate results to the active interpretation path, so Work, replica and status
// decisions remain identical whether a runner is configured or not.
type ShadowRunner interface {
	// SubmitGetReplicas enqueues a shadow evaluation for the InterpretReplica operation.
	SubmitGetReplicas(object *unstructured.Unstructured, activeReplica int32, activeRequires *workv1alpha2.ReplicaRequirements, activeErr error, activeScriptVersion string)

	// SubmitInterpretHealth enqueues a shadow evaluation for the InterpretHealth operation.
	SubmitInterpretHealth(object *unstructured.Unstructured, healthy bool, activeErr error, activeScriptVersion string)

	// SubmitReviseReplica enqueues a shadow evaluation for the ReviseReplica operation.
	SubmitReviseReplica(object, revised *unstructured.Unstructured, desiredReplica int64, activeErr error, activeScriptVersion string)
}

// Compile-time check that the shadow interpreter satisfies the ShadowRunner contract.
var _ ShadowRunner = (*shadow.Interpreter)(nil)

// ConfigurableInterpreter interprets resources with resource interpreter customizations.
type ConfigurableInterpreter struct {
	// configManager caches all ResourceInterpreterCustomizations.
	configManager configmanager.ConfigManager
	luaVM         *luavm.VM
	// shadow is optional. When nil, interpretation behaves exactly as before the
	// shadow feature existed. It only observes active calls and never feeds results back.
	shadow ShadowRunner
}

// NewConfigurableInterpreter builds a new interpreter by registering the
// event handler to the provided informer instance.
func NewConfigurableInterpreter(informer genericmanager.SingleClusterInformerManager) *ConfigurableInterpreter {
	return NewConfigurableInterpreterWithShadow(informer, nil)
}

// NewConfigurableInterpreterWithShadow builds a new interpreter with an optional
// shadow runner. A nil runner disables shadow interpretation entirely.
func NewConfigurableInterpreterWithShadow(informer genericmanager.SingleClusterInformerManager, shadowRunner ShadowRunner) *ConfigurableInterpreter {
	return &ConfigurableInterpreter{
		configManager: configmanager.NewInterpreterConfigManager(informer),
		// TODO: set an appropriate pool size.
		luaVM:  luavm.New(false, 10),
		shadow: shadowRunner,
	}
}

// HookEnabled tells if any hook exist for specific resource gvk and operation type.
func (c *ConfigurableInterpreter) HookEnabled(kind schema.GroupVersionKind, operationType configv1alpha1.InterpreterOperation) bool {
	accessor, exist := c.getCustomAccessor(kind)
	if !exist {
		return exist
	}

	if operationType == configv1alpha1.InterpreterOperationInterpretDependency {
		scripts := accessor.GetDependencyInterpretationLuaScripts()
		return scripts != nil
	}

	var script string
	switch operationType {
	case configv1alpha1.InterpreterOperationAggregateStatus:
		script = accessor.GetStatusAggregationLuaScript()
	case configv1alpha1.InterpreterOperationInterpretHealth:
		script = accessor.GetHealthInterpretationLuaScript()
	case configv1alpha1.InterpreterOperationInterpretReplica:
		script = accessor.GetReplicaResourceLuaScript()
	case configv1alpha1.InterpreterOperationInterpretComponent:
		script = accessor.GetComponentResourceLuaScript()
	case configv1alpha1.InterpreterOperationInterpretStatus:
		script = accessor.GetStatusReflectionLuaScript()
	case configv1alpha1.InterpreterOperationRetain:
		script = accessor.GetRetentionLuaScript()
	case configv1alpha1.InterpreterOperationReviseReplica:
		script = accessor.GetReplicaRevisionLuaScript()
	}
	return len(script) > 0
}

// GetReplicas returns the desired replicas of the object as well as the requirements of each replica.
func (c *ConfigurableInterpreter) GetReplicas(object *unstructured.Unstructured) (replicas int32, requires *workv1alpha2.ReplicaRequirements, enabled bool, err error) {
	accessor, enabled := c.getCustomAccessor(object.GroupVersionKind())
	if !enabled {
		return
	}

	script := accessor.GetReplicaResourceLuaScript()
	if len(script) == 0 {
		enabled = false
		return
	}

	klog.V(4).Infof("Running operation %s for object: %v %s/%s with configurable interpreter.",
		configv1alpha1.InterpreterOperationInterpretReplica, object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	replicas, requires, err = c.luaVM.GetReplicas(object, script)
	// The shadow runner is purely observational: its submission is non-blocking and
	// its result can never alter the replicas returned to the controllers.
	if c.shadow != nil {
		c.shadow.SubmitGetReplicas(object, replicas, requires, err, scriptDigest(script))
	}
	return
}

// GetComponents returns the desired components of the object.
func (c *ConfigurableInterpreter) GetComponents(object *unstructured.Unstructured) (components []workv1alpha2.Component, enabled bool, err error) {
	accessor, enabled := c.getCustomAccessor(object.GroupVersionKind())
	if !enabled {
		return
	}

	script := accessor.GetComponentResourceLuaScript()
	if len(script) == 0 {
		enabled = false
		return
	}

	klog.V(4).Infof("Running operation %s for object: %v %s/%s with configurable interpreter.",
		configv1alpha1.InterpreterOperationInterpretComponent, object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	components, err = c.luaVM.GetComponents(object, script)
	return
}

// ReviseReplica revises the replica of the given object.
func (c *ConfigurableInterpreter) ReviseReplica(object *unstructured.Unstructured, replica int64) (revised *unstructured.Unstructured, enabled bool, err error) {
	accessor, enabled := c.getCustomAccessor(object.GroupVersionKind())
	if !enabled {
		return
	}

	script := accessor.GetReplicaRevisionLuaScript()
	if len(script) == 0 {
		enabled = false
		return
	}

	klog.V(4).Infof("Running operation %s for object: %v %s/%s with configurable interpreter.",
		configv1alpha1.InterpreterOperationReviseReplica, object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	revised, err = c.luaVM.ReviseReplica(object, replica, script)
	// The shadow runner only observes the active revision; the revised object applied
	// to the cluster always comes from the active VM, never from the candidate script.
	if c.shadow != nil {
		c.shadow.SubmitReviseReplica(object, revised, replica, err, scriptDigest(script))
	}
	return
}

// Retain returns the objects that based on the "desired" object but with values retained from the "observed" object.
func (c *ConfigurableInterpreter) Retain(desired *unstructured.Unstructured, observed *unstructured.Unstructured) (retained *unstructured.Unstructured, enabled bool, err error) {
	accessor, enabled := c.getCustomAccessor(desired.GroupVersionKind())
	if !enabled {
		return
	}

	script := accessor.GetRetentionLuaScript()
	if len(script) == 0 {
		enabled = false
		return
	}

	klog.V(4).Infof("Running operation %s for object: %v %s/%s with configurable interpreter.",
		configv1alpha1.InterpreterOperationRetain, desired.GroupVersionKind(), desired.GetNamespace(), desired.GetName())
	retained, err = c.luaVM.Retain(desired, observed, script)
	return
}

// AggregateStatus returns the objects that based on the 'object' but with status aggregated.
func (c *ConfigurableInterpreter) AggregateStatus(object *unstructured.Unstructured, aggregatedStatusItems []workv1alpha2.AggregatedStatusItem) (status *unstructured.Unstructured, enabled bool, err error) {
	accessor, enabled := c.getCustomAccessor(object.GroupVersionKind())
	if !enabled {
		return
	}

	script := accessor.GetStatusAggregationLuaScript()
	if len(script) == 0 {
		enabled = false
		return
	}

	klog.V(4).Infof("Running operation %s for object: %v %s/%s with configurable interpreter.",
		configv1alpha1.InterpreterOperationAggregateStatus, object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	status, err = c.luaVM.AggregateStatus(object, aggregatedStatusItems, script)
	return
}

// GetDependencies returns the dependent resources of the given object.
func (c *ConfigurableInterpreter) GetDependencies(object *unstructured.Unstructured) (dependencies []configv1alpha1.DependentObjectReference, enabled bool, err error) {
	accessor, enabled := c.getCustomAccessor(object.GroupVersionKind())
	if !enabled {
		return
	}

	scripts := accessor.GetDependencyInterpretationLuaScripts()
	if scripts == nil {
		enabled = false
		return
	}

	klog.V(4).Infof("Running operation %s for object: %v %s/%s with configurable interpreter.",
		configv1alpha1.InterpreterOperationInterpretDependency, object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	refs := sets.New[configv1alpha1.DependentObjectReference]()
	for _, luaScript := range scripts {
		var references []configv1alpha1.DependentObjectReference
		references, err = c.luaVM.GetDependencies(object, luaScript)
		if err != nil {
			klog.Errorf("Failed to get DependentObjectReferences from object: %v %s/%s, error: %v",
				object.GroupVersionKind(), object.GetNamespace(), object.GetName(), err)
			return
		}
		err = validation.VerifyDependencies(references)
		if err != nil {
			return
		}
		refs.Insert(references...)
	}
	dependencies = refs.UnsortedList()

	// keep returned items in the same order between each call.
	sort.Slice(dependencies, func(i, j int) bool {
		if dependencies[i].APIVersion != dependencies[j].APIVersion {
			return dependencies[i].APIVersion < dependencies[j].APIVersion
		}
		if dependencies[i].Kind != dependencies[j].Kind {
			return dependencies[i].Kind < dependencies[j].Kind
		}
		if dependencies[i].Namespace != dependencies[j].Namespace {
			return dependencies[i].Namespace < dependencies[j].Namespace
		}
		return dependencies[i].Name < dependencies[j].Name
	})
	return
}

// ReflectStatus returns the status of the object.
func (c *ConfigurableInterpreter) ReflectStatus(object *unstructured.Unstructured) (status *runtime.RawExtension, enabled bool, err error) {
	accessor, enabled := c.getCustomAccessor(object.GroupVersionKind())
	if !enabled {
		return
	}

	script := accessor.GetStatusReflectionLuaScript()
	if len(script) == 0 {
		enabled = false
		return
	}

	klog.V(4).Infof("Running operation %s for object: %v %s/%s with configurable interpreter.",
		configv1alpha1.InterpreterOperationInterpretStatus, object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	status, err = c.luaVM.ReflectStatus(object, script)
	return
}

// InterpretHealth returns the health state of the object.
func (c *ConfigurableInterpreter) InterpretHealth(object *unstructured.Unstructured) (health bool, enabled bool, err error) {
	accessor, enabled := c.getCustomAccessor(object.GroupVersionKind())
	if !enabled {
		return
	}

	script := accessor.GetHealthInterpretationLuaScript()
	if len(script) == 0 {
		enabled = false
		return
	}

	klog.V(4).Infof("Running operation %s for object: %v %s/%s with configurable interpreter.",
		configv1alpha1.InterpreterOperationInterpretHealth, object.GroupVersionKind(), object.GetNamespace(), object.GetName())
	health, err = c.luaVM.InterpretHealth(object, script)
	// Shadow health evaluations are diagnostics only; the health state consumed by
	// status controllers is always the active result.
	if c.shadow != nil {
		c.shadow.SubmitInterpretHealth(object, health, err, scriptDigest(script))
	}
	return
}

// scriptDigest returns a bounded sha256 prefix of a Lua script, usable as a stable
// version label without retaining the script source in diagnostics.
func scriptDigest(script string) string {
	sum := sha256.Sum256([]byte(script))
	return hex.EncodeToString(sum[:])[:12]
}

func (c *ConfigurableInterpreter) getCustomAccessor(kind schema.GroupVersionKind) (configmanager.CustomAccessor, bool) {
	if !c.configManager.HasSynced() {
		klog.Errorf("not yet ready to handle request")
		return nil, false
	}

	accessor, exist := c.configManager.CustomAccessors()[kind]
	return accessor, exist
}

// LoadConfig loads and stores rules from customizations
func (c *ConfigurableInterpreter) LoadConfig(customizations []*configv1alpha1.ResourceInterpreterCustomization) {
	c.configManager.LoadConfig(customizations)
}
