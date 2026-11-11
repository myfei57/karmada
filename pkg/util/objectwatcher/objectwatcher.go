/*
Copyright 2021 The Karmada Authors.

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
	"errors"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/default/native/prune"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/keys"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/restmapper"
)

// OperationResult is the action result of an Update call.
type OperationResult string

const (
	// OperationResultNone means that the update operation was not performed and the resource has not been changed.
	OperationResultNone OperationResult = "none"
	// OperationResultUnchanged means that the update operation was performed but the resource did not change.
	OperationResultUnchanged OperationResult = "unchanged"
	// OperationResultUpdated means that an existing resource is updated.
	OperationResultUpdated OperationResult = "updated"
	// OperationResultCreated means that a new resource was created through a member API call.
	OperationResultCreated OperationResult = "created"
	// OperationResultObserved means that no write happened and the evidence was obtained from a direct
	// member API readback that passed identity and ownership preconditions.
	OperationResultObserved OperationResult = "observed"
)

// ErrResourceConflict is the sentinel error wrapped when an existing member cluster object
// conflicts with the desired object because Karmada does not own it or its Work reference differs.
var ErrResourceConflict = errors.New("resource ownership or identity conflict")

// ApplyConflictError describes a member cluster object that failed identity or ownership preconditions.
// Callers can match it with errors.Is(err, ErrResourceConflict).
type ApplyConflictError struct {
	GVK       string
	Namespace string
	Name      string
	Detail    string
}

func (e *ApplyConflictError) Error() string {
	return fmt.Sprintf("%v: resource(%s %s/%s): %s", ErrResourceConflict, e.GVK, e.Namespace, e.Name, e.Detail)
}

func (e *ApplyConflictError) Unwrap() error {
	return ErrResourceConflict
}

// ApplyOutcome is the result of a create/update attempt against a member cluster.
// Object is always sourced from a real member API write response or a precondition-checked
// readback; it must never be populated from an informer cache object when Readback is false
// and Operation indicates a write (Created/Updated).
type ApplyOutcome struct {
	// Operation classifies how Object was obtained.
	Operation OperationResult
	// Object is the member cluster object carrying the server-confirmed UID and resourceVersion.
	Object *unstructured.Unstructured
	// Readback is true when Object was obtained from a direct server GET under identity
	// and ownership preconditions rather than from a write response.
	Readback bool
}

// ObjectWatcher manages operations for object dispatched to member clusters.
type ObjectWatcher interface {
	Create(ctx context.Context, clusterName string, desireObj *unstructured.Unstructured) (*ApplyOutcome, error)
	Update(ctx context.Context, clusterName string, desireObj, clusterObj *unstructured.Unstructured) (*ApplyOutcome, error)
	Delete(ctx context.Context, clusterName string, desireObj *unstructured.Unstructured) error
	GetVersionRecord(clusterName string, object client.Object) (string, bool)
}

type objectWatcherImpl struct {
	Lock                 sync.RWMutex
	RESTMapper           meta.RESTMapper
	KubeClientSet        client.Client
	VersionRecord        map[string]map[string]string
	ClusterClientSetFunc util.NewClusterDynamicClientSetFunc
	ClusterClientOption  *util.ClientOption
	resourceInterpreter  resourceinterpreter.ResourceInterpreter
	InformerManager      genericmanager.MultiClusterInformerManager
}

// NewObjectWatcher returns an instance of ObjectWatcher
func NewObjectWatcher(kubeClientSet client.Client, restMapper meta.RESTMapper, clusterClientSetFunc util.NewClusterDynamicClientSetFunc, clusterClientOption *util.ClientOption, interpreter resourceinterpreter.ResourceInterpreter, informerManager genericmanager.MultiClusterInformerManager) ObjectWatcher {
	return &objectWatcherImpl{
		KubeClientSet:        kubeClientSet,
		VersionRecord:        make(map[string]map[string]string),
		RESTMapper:           restMapper,
		ClusterClientSetFunc: clusterClientSetFunc,
		ClusterClientOption:  clusterClientOption,
		resourceInterpreter:  interpreter,
		InformerManager:      informerManager,
	}
}

func (o *objectWatcherImpl) Create(ctx context.Context, clusterName string, desireObj *unstructured.Unstructured) (*ApplyOutcome, error) {
	dynamicClusterClient, err := o.ClusterClientSetFunc(clusterName, o.KubeClientSet, o.ClusterClientOption)
	if err != nil {
		klog.Errorf("Failed to build dynamic cluster client for cluster %s, err: %v.", clusterName, err)
		return nil, err
	}

	gvr, err := restmapper.GetGroupVersionResource(o.RESTMapper, desireObj.GroupVersionKind())
	if err != nil {
		klog.Errorf("Failed to create resource(kind=%s, %s/%s) in cluster %s as mapping GVK to GVR failed: %v", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName, err)
		return nil, err
	}

	fedKey, err := keys.FederatedKeyFunc(clusterName, desireObj)
	if err != nil {
		klog.Errorf("Failed to get FederatedKey %s, error: %v.", desireObj.GetName(), err)
		return nil, err
	}
	_, err = helper.GetObjectFromCache(o.RESTMapper, o.InformerManager, fedKey)
	if err == nil {
		// The object is already known to the informer cache. Never claim a write from a cache hit:
		// when receipts are enabled, confirm fresh identity/version through a direct readback;
		// otherwise preserve the legacy no-API-call behavior.
		if !features.WorkApplyReceiptsEnabled() {
			return &ApplyOutcome{Operation: OperationResultNone, Object: nil}, nil
		}
		return o.readback(ctx, dynamicClusterClient, gvr, desireObj, OperationResultObserved)
	} else if !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to get resource(kind=%s, %s/%s) from member cluster, err is %v.", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), err)
		return nil, err
	}

	clusterObj, createErr := dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Create(ctx, desireObj, metav1.CreateOptions{})
	if createErr == nil {
		klog.Infof("Created the resource(kind=%s, %s/%s) on cluster(%s).", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName)
		// record version
		o.recordVersion(clusterObj, dynamicClusterClient.ClusterName)
		return &ApplyOutcome{Operation: OperationResultCreated, Object: clusterObj}, nil
	}

	klog.Errorf("Failed to create resource(kind=%s, %s/%s) in cluster %s, err is %v.", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName, createErr)
	if apierrors.IsAlreadyExists(createErr) {
		// The cache lagged behind the member API (or a previous attempt landed before an error was returned).
		// Resolve through a direct readback with identity/ownership preconditions instead of overwriting.
		return o.readback(ctx, dynamicClusterClient, gvr, desireObj, OperationResultObserved)
	}
	// The write result is ambiguous (timeouts, connection resets ...): verify whether the object
	// landed before deciding, so retries do not repeat an unknown side-effecting create.
	return o.readbackAfterAmbiguousWrite(ctx, dynamicClusterClient, gvr, desireObj, createErr)
}

func (o *objectWatcherImpl) retainClusterFields(desired, observed *unstructured.Unstructured) *unstructured.Unstructured {
	// Pass the same ResourceVersion as in the cluster object for update operation, otherwise operation will fail.
	desired.SetResourceVersion(observed.GetResourceVersion())

	// Retain finalizers since they will typically be set by
	// controllers in a member cluster.  It is still possible to set the fields
	// via overrides.
	desired.SetFinalizers(observed.GetFinalizers())

	// Retain ownerReferences since they will typically be set by controllers in a member cluster.
	desired.SetOwnerReferences(observed.GetOwnerReferences())

	// Retain annotations since they will typically be set by controllers in a member cluster
	// and be set by user in karmada-controller-plane.
	util.RetainAnnotations(desired, observed)

	// Retain labels since they will typically be set by controllers in a member cluster
	// and be set by user in karmada-controller-plane.
	util.RetainLabels(desired, observed)

	return desired
}

func (o *objectWatcherImpl) Update(ctx context.Context, clusterName string, desireObj, clusterObj *unstructured.Unstructured) (*ApplyOutcome, error) {
	updateAllowed := o.allowUpdate(clusterName, desireObj, clusterObj)
	if !updateAllowed {
		// The existing resource is not managed by Karmada, and no conflict resolution found, avoid updating the existing resource by default.
		return nil, fmt.Errorf("resource(kind=%s, %s/%s) already exists in the cluster %v and the %s strategy value is empty, Karmada will not manage this resource",
			desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName, workv1alpha2.ResourceConflictResolutionAnnotation)
	}

	dynamicClusterClient, err := o.ClusterClientSetFunc(clusterName, o.KubeClientSet, o.ClusterClientOption)
	if err != nil {
		klog.Errorf("Failed to build dynamic cluster client for cluster %s, err: %v.", clusterName, err)
		return nil, err
	}

	gvr, err := restmapper.GetGroupVersionResource(o.RESTMapper, desireObj.GroupVersionKind())
	if err != nil {
		klog.Errorf("Failed to update the resource(kind=%s, %s/%s) in the cluster %s as mapping GVK to GVR failed: %v", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName, err)
		return nil, err
	}

	desireObj = o.retainClusterFields(desireObj, clusterObj)
	if o.resourceInterpreter.HookEnabled(desireObj.GroupVersionKind(), configv1alpha1.InterpreterOperationRetain) {
		desireObj, err = o.resourceInterpreter.Retain(desireObj, clusterObj)
		if err != nil {
			klog.Errorf("Failed to retain fields for resource(kind=%s, %s/%s) in cluster %s: %v", clusterObj.GetKind(), clusterObj.GetNamespace(), clusterObj.GetName(), clusterName, err)
			return nil, err
		}
	}

	// If there's no actual content changes, skip the update and record the current version.
	// Whether the DeepEqual check returns true will depend on the implementation of the member cluster's mutating webhook.
	// In theory, if the member cluster's mutating webhook modifies the content on every update,
	// then this DeepEqual check may always return false. So the objectWatcher's VersionRecord is still needed.
	if o.resourceDeepEqual(desireObj, clusterObj, clusterName) {
		klog.Infof("No need to update resource(kind=%s, %s/%s) in cluster %s because the content has not changed", clusterObj.GetKind(), clusterObj.GetNamespace(), clusterObj.GetName(), clusterName)
		if !features.WorkApplyReceiptsEnabled() {
			o.recordVersion(clusterObj, clusterName)
			return &ApplyOutcome{Operation: OperationResultNone, Object: clusterObj}, nil
		}
		// The cached object may lag behind the member API. Fetch fresh evidence directly so the
		// receipt never fabricates UID/resourceVersion from a stale cache object.
		return o.readback(ctx, dynamicClusterClient, gvr, desireObj, OperationResultObserved)
	}

	resource, err := dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Update(ctx, desireObj, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update resource(kind=%s, %s/%s) in cluster %s, err: %v.", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName, err)
		if apierrors.IsConflict(err) {
			// Stale resourceVersion: retry with a fresh informer object.
			return nil, err
		}
		return o.readbackAfterAmbiguousUpdate(ctx, dynamicClusterClient, gvr, desireObj, clusterName, err)
	}

	// record version
	// TODO: Occasionally, due to the nature of asynchronous processing, the event generated by an object update may
	// 		reach the informer before the corresponding recording operation is executed.
	//      This can sometimes lead to redundant reconciliation by the execution controller.
	o.recordVersion(resource, clusterName)

	if clusterObj.GetResourceVersion() == resource.GetResourceVersion() {
		klog.Infof("Updated the resource(kind=%s, %s/%s) on cluster(%s) but the cluster object was not changed.", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName)
		return &ApplyOutcome{Operation: OperationResultUnchanged, Object: resource}, nil
	}

	klog.Infof("Updated the resource(kind=%s, %s/%s) on cluster(%s).", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName)
	return &ApplyOutcome{Operation: OperationResultUpdated, Object: resource}, nil
}

func (o *objectWatcherImpl) Delete(ctx context.Context, clusterName string, desireObj *unstructured.Unstructured) error {
	fedKey, err := keys.FederatedKeyFunc(clusterName, desireObj)
	if err != nil {
		klog.Errorf("Failed to get FederatedKey %s, error: %v", desireObj.GetName(), err)
		return err
	}

	clusterObj, err := helper.GetObjectFromCache(o.RESTMapper, o.InformerManager, fedKey)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("Failed to get the resource(kind=%s, %s/%s) from the member cluster %s, err is %v", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName, err)
		return err
	}

	// Avoid deleting resources that are not managed by Karmada.
	if !o.isManagedResource(clusterObj) {
		klog.Infof("Abort deleting the resource(kind=%s, %s/%s) which exists in the member cluster %s but is not managed by Karmada.", clusterObj.GetKind(), clusterObj.GetNamespace(), clusterObj.GetName(), clusterName)
		return nil
	}

	dynamicClusterClient, err := o.ClusterClientSetFunc(clusterName, o.KubeClientSet, o.ClusterClientOption)
	if err != nil {
		klog.Errorf("Failed to build dynamic cluster client for cluster %s, err: %v", clusterName, err)
		return err
	}

	gvr, err := restmapper.GetGroupVersionResource(o.RESTMapper, desireObj.GroupVersionKind())
	if err != nil {
		klog.Errorf("Failed to delete the resource(kind=%s, %s/%s) in the cluster %s as mapping GVK to GVR failed: %v", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName, err)
		return err
	}

	// Set deletion strategy to background explicitly even though it's the default strategy for most of the resources.
	// The reason for this is to fix the exception case that Kubernetes does on Job(batch/v1).
	// In kubernetes, the Job's default deletion strategy is "Orphan", that will cause the "Pods" created by
	// the Job still exist after the Job has been deleted.
	// Refer to https://github.com/karmada-io/karmada/issues/969 for more details.
	deleteBackground := metav1.DeletePropagationBackground
	deleteOption := metav1.DeleteOptions{
		PropagationPolicy: &deleteBackground,
	}

	err = dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Delete(ctx, desireObj.GetName(), deleteOption)
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete the resource(kind=%s, %s/%s) in the cluster %s, err: %v.", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName, err)
		return err
	}
	klog.Infof("Deleted the resource(kind=%s, %s/%s) on cluster(%s).", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName)

	objectKey := o.genObjectKey(desireObj)
	o.deleteVersionRecord(dynamicClusterClient.ClusterName, objectKey)

	return nil
}

func (o *objectWatcherImpl) genObjectKey(obj *unstructured.Unstructured) string {
	return obj.GroupVersionKind().String() + "/" + obj.GetNamespace() + "/" + obj.GetName()
}

// recordVersion will add or update resource version records
func (o *objectWatcherImpl) recordVersion(clusterObj *unstructured.Unstructured, clusterName string) {
	objVersion := clusterObj.GetResourceVersion()
	objectKey := o.genObjectKey(clusterObj)

	o.Lock.Lock()
	defer o.Lock.Unlock()
	if o.VersionRecord[clusterName] == nil {
		o.VersionRecord[clusterName] = make(map[string]string)
	}
	o.VersionRecord[clusterName][objectKey] = objVersion
}

// getVersionRecord will return the recorded version of given resource(if exist)
func (o *objectWatcherImpl) getVersionRecord(clusterName, resourceName string) (string, bool) {
	o.Lock.RLock()
	defer o.Lock.RUnlock()

	version, exist := o.VersionRecord[clusterName][resourceName]
	return version, exist
}

// deleteVersionRecord will delete the recorded version of given resource
func (o *objectWatcherImpl) deleteVersionRecord(clusterName, resourceName string) {
	o.Lock.Lock()
	defer o.Lock.Unlock()
	delete(o.VersionRecord[clusterName], resourceName)
}

func (o *objectWatcherImpl) GetVersionRecord(clusterName string, object client.Object) (string, bool) {
	return o.getVersionRecord(clusterName, object.GetObjectKind().GroupVersionKind().String()+"/"+object.GetNamespace()+"/"+object.GetName())
}

func (o *objectWatcherImpl) isManagedResource(clusterObj *unstructured.Unstructured) bool {
	return util.GetLabelValue(clusterObj.GetLabels(), util.ManagedByKarmadaLabel) == util.ManagedByKarmadaLabelValue
}

func (o *objectWatcherImpl) allowUpdate(clusterName string, desiredObj, clusterObj *unstructured.Unstructured) bool {
	// If the existing resource is managed by Karmada, then the updating is allowed.
	if o.isManagedResource(clusterObj) {
		return true
	}
	klog.Warningf("The existing resource(kind=%s, %s/%s) in the cluster(%s) is not managed by Karmada.",
		clusterObj.GetKind(), clusterObj.GetNamespace(), clusterObj.GetName(), clusterName)

	// This happens when promoting workload to the Karmada control plane.
	conflictResolution := util.GetAnnotationValue(desiredObj.GetAnnotations(), workv1alpha2.ResourceConflictResolutionAnnotation)
	if conflictResolution == workv1alpha2.ResourceConflictResolutionOverwrite {
		klog.Infof("Force overwriting of the resource(kind=%s, %s/%s) in the cluster(%s).",
			desiredObj.GetKind(), desiredObj.GetNamespace(), desiredObj.GetName(), clusterName)
		return true
	}

	return false
}

// resourceDeepEqual checks if the desired object and the cluster object are equal.
func (o *objectWatcherImpl) resourceDeepEqual(desiredObj, clusterObj *unstructured.Unstructured, clusterName string) bool {
	clusterObjCopy := clusterObj.DeepCopy()
	// Remove fields that should not be propagated to member clusters,
	// applying the same pruning strategy as used for the desired object.
	err := prune.RemoveIrrelevantFields(clusterObjCopy, prune.RemoveJobTTLSeconds)
	if err != nil {
		klog.Warningf("Failed to remove irrelevant fields for cluster resource(kind=%s, %s/%s) in cluster %s: %v", clusterObj.GetKind(), clusterObj.GetNamespace(), clusterObj.GetName(), clusterName, err)
		return false
	}

	// Retain fields that are mandatory for update operations,
	// applying the same retain strategy as used ofr the desired object.
	clusterObjCopy = o.retainClusterFields(clusterObjCopy, clusterObj)

	return equality.Semantic.DeepEqual(desiredObj, clusterObjCopy)
}

// verifyIdentityAndOwnership enforces the preconditions under which a readback may be trusted:
// the observed object must share GVK/namespace/name with the desired object and must be owned by
// the same Karmada Work. Any mismatch is reported as an explicit conflict instead of being swallowed.
func verifyIdentityAndOwnership(desired, observed *unstructured.Unstructured) error {
	if observed == nil {
		return fmt.Errorf("readback returned no object")
	}
	if desired.GetAPIVersion() != observed.GetAPIVersion() || desired.GetKind() != observed.GetKind() ||
		desired.GetNamespace() != observed.GetNamespace() || desired.GetName() != observed.GetName() {
		return &ApplyConflictError{
			GVK:       observed.GroupVersionKind().String(),
			Namespace: observed.GetNamespace(),
			Name:      observed.GetName(),
			Detail:    fmt.Sprintf("identity mismatch, desired %s/%s/%s", desired.GetAPIVersion(), desired.GetKind(), desired.GetName()),
		}
	}

	if util.GetLabelValue(observed.GetLabels(), util.ManagedByKarmadaLabel) != util.ManagedByKarmadaLabelValue {
		return &ApplyConflictError{
			GVK:       observed.GroupVersionKind().String(),
			Namespace: observed.GetNamespace(),
			Name:      observed.GetName(),
			Detail:    "existing object is not managed by Karmada",
		}
	}

	desiredWorkNamespace := util.GetAnnotationValue(desired.GetAnnotations(), workv1alpha2.WorkNamespaceAnnotation)
	desiredWorkName := util.GetAnnotationValue(desired.GetAnnotations(), workv1alpha2.WorkNameAnnotation)
	if desiredWorkNamespace != "" || desiredWorkName != "" {
		observedWorkNamespace := util.GetAnnotationValue(observed.GetAnnotations(), workv1alpha2.WorkNamespaceAnnotation)
		observedWorkName := util.GetAnnotationValue(observed.GetAnnotations(), workv1alpha2.WorkNameAnnotation)
		if desiredWorkNamespace != observedWorkNamespace || desiredWorkName != observedWorkName {
			return &ApplyConflictError{
				GVK:       observed.GroupVersionKind().String(),
				Namespace: observed.GetNamespace(),
				Name:      observed.GetName(),
				Detail: fmt.Sprintf("Work reference mismatch, desired %s/%s but observed %s/%s",
					desiredWorkNamespace, desiredWorkName, observedWorkNamespace, observedWorkName),
			}
		}
	}
	return nil
}

// readback performs a direct member API GET and returns verified evidence without performing any write.
func (o *objectWatcherImpl) readback(ctx context.Context, dynamicClusterClient *util.DynamicClusterClient,
	gvr schema.GroupVersionResource, desireObj *unstructured.Unstructured, operation OperationResult) (*ApplyOutcome, error) {
	observed, err := dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Get(ctx, desireObj.GetName(), metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Readback of resource(kind=%s, %s/%s) in cluster %s failed, err: %v.", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), dynamicClusterClient.ClusterName, err)
		return nil, err
	}
	if err := verifyIdentityAndOwnership(desireObj, observed); err != nil {
		klog.Error(err)
		return nil, err
	}
	o.recordVersion(observed, dynamicClusterClient.ClusterName)
	return &ApplyOutcome{Operation: operation, Object: observed, Readback: true}, nil
}

// readbackAfterAmbiguousWrite resolves a Create call whose result is unknown. A verified readback
// proves the create landed; a conflict is surfaced explicitly; a missing object means the original
// error must be retried rather than issuing another blind create.
func (o *objectWatcherImpl) readbackAfterAmbiguousWrite(ctx context.Context, dynamicClusterClient *util.DynamicClusterClient,
	gvr schema.GroupVersionResource, desireObj *unstructured.Unstructured, writeErr error) (*ApplyOutcome, error) {
	observed, err := dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Get(ctx, desireObj.GetName(), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, writeErr
		}
		return nil, err
	}
	if verifyErr := verifyIdentityAndOwnership(desireObj, observed); verifyErr != nil {
		klog.Error(verifyErr)
		return nil, verifyErr
	}
	klog.Infof("Ambiguous create of resource(kind=%s, %s/%s) on cluster(%s) confirmed via readback.", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), dynamicClusterClient.ClusterName)
	o.recordVersion(observed, dynamicClusterClient.ClusterName)
	return &ApplyOutcome{Operation: OperationResultObserved, Object: observed, Readback: true}, nil
}

// readbackAfterAmbiguousUpdate resolves an Update call whose result is unknown. When the observed
// content already matches the desired content the update is considered landed; otherwise the original
// write error is returned so the controller retries with fresh state.
func (o *objectWatcherImpl) readbackAfterAmbiguousUpdate(ctx context.Context, dynamicClusterClient *util.DynamicClusterClient,
	gvr schema.GroupVersionResource, desireObj *unstructured.Unstructured, clusterName string, writeErr error) (*ApplyOutcome, error) {
	observed, err := dynamicClusterClient.DynamicClientSet.Resource(gvr).Namespace(desireObj.GetNamespace()).Get(ctx, desireObj.GetName(), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, writeErr
		}
		return nil, err
	}
	if verifyErr := verifyIdentityAndOwnership(desireObj, observed); verifyErr != nil {
		klog.Error(verifyErr)
		return nil, verifyErr
	}
	if !o.resourceDeepEqual(desireObj, observed, clusterName) {
		return nil, writeErr
	}
	klog.Infof("Ambiguous update of resource(kind=%s, %s/%s) on cluster(%s) confirmed via readback.", desireObj.GetKind(), desireObj.GetNamespace(), desireObj.GetName(), clusterName)
	o.recordVersion(observed, clusterName)
	return &ApplyOutcome{Operation: OperationResultObserved, Object: observed, Readback: true}, nil
}
