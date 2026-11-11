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

package work

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/default/native/prune"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/applyreceipt"
)

// prunableFieldPaths lists the server-managed metadata/status paths that
// prune.RemoveIrrelevantFields removes when present. It is used to summarize
// the webhook mutations separately from the original request.
var prunableFieldPaths = [][]string{
	{"metadata", "creationTimestamp"},
	{"metadata", "deletionTimestamp"},
	{"metadata", "deletionGracePeriodSeconds"},
	{"metadata", "generation"},
	{"metadata", "managedFields"},
	{"metadata", "resourceVersion"},
	{"metadata", "selfLink"},
	{"metadata", "uid"},
	{"metadata", "ownerReferences"},
	{"metadata", "finalizers"},
	{"status"},
}

// MutatingAdmission mutates API request if necessary.
type MutatingAdmission struct {
	Decoder admission.Decoder
}

// Check if our MutatingAdmission implements necessary interface
var _ admission.Handler = &MutatingAdmission{}

// Handle yields a response to an AdmissionRequest.
// This function could affect the informer cache of controller-manager.
// In controller-manager, redundant updates to Work are avoided as much as possible, and changes to informer cache will affect this behavior.
// Therefore, if someone modify this function, please ensure that the relevant logic in controller-manager also be updated.
func (a *MutatingAdmission) Handle(_ context.Context, req admission.Request) admission.Response {
	work := &workv1alpha1.Work{}

	err := a.Decoder.Decode(req, work)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	klog.V(2).Infof("Mutating the work(%s/%s) for request: %s", work.Namespace, work.Name, req.Operation)

	if util.GetLabelValue(work.Labels, workv1alpha2.WorkPermanentIDLabel) == "" {
		util.MergeLabel(work, workv1alpha2.WorkPermanentIDLabel, uuid.New().String())
	}

	var manifests []workv1alpha1.Manifest

	for _, manifest := range work.Spec.Workload.Manifests {
		workloadObj := &unstructured.Unstructured{}
		err := json.Unmarshal(manifest.Raw, workloadObj)
		if err != nil {
			klog.Errorf("Failed to unmarshal the work(%s/%s) manifest to Unstructured, err: %v", work.Namespace, work.Name, err)
			return admission.Errored(http.StatusInternalServerError, err)
		}

		var mutated workv1alpha1.Manifest
		if features.WorkApplyReceiptsEnabled() {
			mutated, err = mutateManifestWithReceipts(work, workloadObj)
		} else {
			mutated, err = mutateManifest(work, workloadObj)
		}
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, err)
		}
		manifests = append(manifests, mutated)
	}

	work.Spec.Workload.Manifests = manifests
	marshaledBytes, err := json.Marshal(work)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledBytes)
}

// mutateManifest applies the legacy pruning and Karmada label/annotation injection.
func mutateManifest(work *workv1alpha1.Work, workloadObj *unstructured.Unstructured) workv1alpha1.Manifest {
	if err := prune.RemoveIrrelevantFields(workloadObj, prune.RemoveJobTTLSeconds); err != nil {
		klog.Errorf("Failed to remove irrelevant fields for the work(%s/%s), err: %v", work.Namespace, work.Name, err)
		return workv1alpha1.Manifest{}
	}

	util.SetLabelsAndAnnotationsForWorkload(workloadObj, work)

	workloadJSON, err := workloadObj.MarshalJSON()
	if err != nil {
		klog.Errorf("Failed to marshal workload of the work(%s/%s), err: %s", work.Namespace, work.Name, err)
		return workv1alpha1.Manifest{}
	}
	return workv1alpha1.Manifest{RawExtension: runtime.RawExtension{Raw: workloadJSON}}
}

// mutateManifestWithReceipt performs the same pruning/injection as mutateManifest and
// additionally records the original-request digest, the applied digest and a size-capped
// mutation summary as workload annotations. Secret plaintext is redacted before digesting
// and never appears in any of the recorded annotations.
func mutateManifestWithReceipts(work *workv1alpha1.Work, workloadObj *unstructured.Unstructured) workv1alpha1.Manifest {
	requestDigest, err := applyreceipt.DigestObject(workloadObj)
	if err != nil {
		klog.Errorf("Failed to compute request digest for the work(%s/%s), err: %v", work.Namespace, work.Name, err)
		return workv1alpha1.Manifest{}
	}

	originalLabels := applyreceipt.SortedStringKeys(workloadObj.GetLabels())
	originalAnnotations := applyreceipt.SortedStringKeys(workloadObj.GetAnnotations())
	removedFields := presentFieldPaths(workloadObj)

	if err := prune.RemoveIrrelevantFields(workloadObj, prune.RemoveJobTTLSeconds); err != nil {
		klog.Errorf("Failed to remove irrelevant fields for the work(%s/%s), err: %v", work.Namespace, work.Name, err)
		return workv1alpha1.Manifest{}
	}
	util.SetLabelsAndAnnotationsForWorkload(workloadObj, work)

	appliedDigest, err := applyreceipt.DigestForReceipt(workloadObj)
	if err != nil {
		klog.Errorf("Failed to compute applied digest for the work(%s/%s), err: %v", work.Namespace, work.Name, err)
		return workv1alpha1.Manifest{}
	}

	summary := &workv1alpha1.ManifestMutationSummary{
		AddedLabels:      addedKeys(originalLabels, applyreceipt.SortedStringKeys(workloadObj.GetLabels())),
		AddedAnnotations: addedKeys(originalAnnotations, applyreceipt.SortedStringKeys(workloadObj.GetAnnotations())),
		RemovedFields:    removedFields,
	}
	summaryRaw, err := applyreceipt.MarshalMutationSummary(summary, workv1alpha1.MaxMutationSummaryLength)
	if err != nil {
		klog.Errorf("Failed to marshal mutation summary for the work(%s/%s), err: %v", work.Namespace, work.Name, err)
		return workv1alpha1.Manifest{}
	}

	annotations := workloadObj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string, 3)
	}
	annotations[workv1alpha2.WorkRequestDigestAnnotation] = requestDigest
	annotations[workv1alpha2.WorkAppliedDigestAnnotation] = appliedDigest
	annotations[workv1alpha2.WorkMutationSummaryAnnotation] = summaryRaw
	workloadObj.SetAnnotations(annotations)
	// Register the new bookkeeping annotations as managed so drift reconciliation
	// treats them as Karmada-owned rather than member-side additions.
	util.RecordManagedAnnotations(workloadObj)

	workloadJSON, err := workloadObj.MarshalJSON()
	if err != nil {
		klog.Errorf("Failed to marshal workload of the work(%s/%s), err: %s", work.Namespace, work.Name, err)
		return workv1alpha1.Manifest{}
	}
	return workv1alpha1.Manifest{RawExtension: runtime.RawExtension{Raw: workloadJSON}}
}

// presentFieldPaths returns the dotted paths of prunable fields that exist on the object.
func presentFieldPaths(obj *unstructured.Unstructured) []string {
	var present []string
	for _, path := range prunableFieldPaths {
		if _, found, _ := unstructured.NestedFieldNoCopy(obj.Object, path...); found {
			present = append(present, joinFieldPath(path))
		}
	}
	return present
}

func joinFieldPath(path []string) string {
	joined := ""
	for i, segment := range path {
		if i > 0 {
			joined += "."
		}
		joined += segment
	}
	return joined
}

// addedKeys returns the keys present in current but absent in original.
func addedKeys(original, current []string) []string {
	originalSet := make(map[string]struct{}, len(original))
	for _, key := range original {
		originalSet[key] = struct{}{}
	}
	var added []string
	for _, key := range current {
		if _, exists := originalSet[key]; !exists {
			added = append(added, key)
		}
	}
	return added
}
