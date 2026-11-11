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

package execution

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter/default/native/prune"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/applyreceipt"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/objectwatcher"
	"github.com/karmada-io/karmada/pkg/util/restmapper"
)

// capReceiptSize enforces the per-receipt byte budget by trimming the message first and
// then confirmed-field entries. Digests, UIDs, identity and reason are never removed.
func capReceiptSize(receipt *workv1alpha1.WorkApplyReceipt) {
	receipt.Message = applyreceipt.TruncateString(receipt.Message, workv1alpha1.MaxWorkApplyReceiptMessageLength)
	for applyreceipt.EstimateReceiptBytes(receipt) > workv1alpha1.MaxWorkApplyReceiptBytes && len(receipt.ConfirmedFields) > 0 {
		receipt.ConfirmedFields = receipt.ConfirmedFields[:len(receipt.ConfirmedFields)-1]
	}
	for applyreceipt.EstimateReceiptBytes(receipt) > workv1alpha1.MaxWorkApplyReceiptBytes && len(receipt.Message) > 0 {
		receipt.Message = receipt.Message[:len(receipt.Message)-1]
	}
}

// persistApplyReceiptsAndCondition derives per-manifest receipts and the WorkApplied condition
// from the same per-manifest result snapshot and writes them in one status CAS update.
// The original apply errors are still returned so failed manifests keep being retried.
func (c *Controller) persistApplyReceiptsAndCondition(ctx context.Context, work *workv1alpha1.Work,
	cluster *clusterv1alpha1.Cluster, results []manifestApplyResult) error {
	receipts := c.buildWorkApplyReceipts(work, cluster, results)

	appliedCount := 0
	var applyErrs []error
	for _, result := range results {
		if result.err != nil {
			applyErrs = append(applyErrs, result.err)
		}
	}
	for _, receipt := range receipts {
		if receipt.Phase == workv1alpha1.WorkApplyReceiptPhaseApplied {
			appliedCount++
		}
	}

	total := len(work.Spec.Workload.Manifests)
	conditionStatus, reason, message := decideAppliedCondition(total, appliedCount, len(receipts), applyErrs)

	statusErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := helper.UpdateStatus(ctx, c.Client, work, func() error {
			// Replace the whole receipt set: only receipts for the current Work generation
			// are retained, so late responses from older generations can never fill new receipts.
			work.Status.WorkApplyReceipts = receipts
			meta.SetStatusCondition(&work.Status.Conditions, metav1.Condition{
				Type:               workv1alpha1.WorkApplied,
				Status:             conditionStatus,
				Reason:             reason,
				Message:            message,
				LastTransitionTime: metav1.Now(),
			})
			return nil
		})
		return err
	})
	if statusErr != nil {
		klog.ErrorS(statusErr, "Failed to persist apply receipts and applied condition", "name", work.Name, "namespace", work.Namespace)
		applyErrs = append(applyErrs, statusErr)
	}
	return utilerrors.NewAggregate(applyErrs)
}

// decideAppliedCondition binds the WorkApplied condition to the receipt evidence.
// The new protocol can only be marked complete when every manifest has an Applied receipt
// for the current generation; missing or Unknown receipts force ConditionFalse.
func decideAppliedCondition(total, appliedCount, receiptCount int, applyErrs []error) (metav1.ConditionStatus, string, string) {
	if total > 0 && receiptCount == 0 {
		return metav1.ConditionFalse, "AppliedFailed",
			"WorkApplyReceipts feature is enabled but no apply receipt was recorded for the current generation"
	}
	if appliedCount == total && len(applyErrs) == 0 {
		return metav1.ConditionTrue, "AppliedSuccessful", "Manifest has been successfully applied"
	}
	detail := "one or more manifests are not confirmed by an apply receipt"
	if aggregate := utilerrors.NewAggregate(applyErrs); aggregate != nil {
		detail = aggregate.Error()
	}
	return metav1.ConditionFalse, "AppliedFailed",
		fmt.Sprintf("Failed to apply all manifests (%d/%d): %s", appliedCount, total, detail)
}

// buildWorkApplyReceipts converts the per-manifest apply results into point-in-time receipts.
// Every receipt is stamped with the current Work UID and generation; outcomes from this sync are
// the only allowed source of observed object UID/resourceVersion.
func (c *Controller) buildWorkApplyReceipts(work *workv1alpha1.Work, cluster *clusterv1alpha1.Cluster,
	results []manifestApplyResult) []workv1alpha1.WorkApplyReceipt {
	receipts := make([]workv1alpha1.WorkApplyReceipt, 0, len(results))
	for _, result := range results {
		receipt := workv1alpha1.WorkApplyReceipt{
			Identifier: workv1alpha1.ResourceIdentifier{
				Ordinal: result.ordinal,
			},
			WorkUID:        work.UID,
			WorkGeneration: work.Generation,
			ClusterUID:     cluster.UID,
			ClusterID:      cluster.Spec.ID,
			ObservedAt:     metav1.Now(),
		}

		if result.workload != nil {
			receipt.Identifier.Group = result.workload.GroupVersionKind().Group
			receipt.Identifier.Version = result.workload.GroupVersionKind().Version
			receipt.Identifier.Kind = result.workload.GetKind()
			receipt.Identifier.Namespace = result.workload.GetNamespace()
			receipt.Identifier.Name = result.workload.GetName()
			receipt.ManifestIdentity = applyreceipt.ManifestIdentity(result.workload)
			receipt.SecretRedacted = applyreceipt.IsSecret(result.workload)
			if gvr, gvrErr := restmapper.GetGroupVersionResource(c.RESTMapper, result.workload.GroupVersionKind()); gvrErr == nil {
				receipt.Identifier.Resource = gvr.Resource
			}
			if digest, digestErr := applyreceipt.DigestForReceipt(result.workload); digestErr == nil {
				receipt.AppliedDigest = digest
			} else {
				klog.ErrorS(digestErr, "Failed to compute applied digest", "ordinal", result.ordinal)
			}
			receipt.RequestDigest = result.workload.GetAnnotations()[workv1alpha2.WorkRequestDigestAnnotation]
			receipt.MutationSummary = extractMutationSummary(result.workload)
		}

		switch {
		case result.err != nil:
			fillUnknownReceipt(&receipt, result.err)
		case result.outcome == nil || result.outcome.Object == nil:
			// Defensive: a nil-error result without a real member object must never look applied.
			receipt.Phase = workv1alpha1.WorkApplyReceiptPhaseUnknown
			receipt.Reason = workv1alpha1.WorkApplyReceiptReasonResultUnknown
			receipt.Message = applyreceipt.TruncateString("apply succeeded without a member cluster object response", workv1alpha1.MaxWorkApplyReceiptMessageLength)
		default:
			c.fillAppliedReceipt(&receipt, result)
		}

		capReceiptSize(&receipt)
		receipts = append(receipts, receipt)
	}
	return receipts
}

// fillAppliedReceipt populates an Applied receipt exclusively from the real member API outcome.
func (c *Controller) fillAppliedReceipt(receipt *workv1alpha1.WorkApplyReceipt, result manifestApplyResult) {
	outcome := result.outcome
	observed := outcome.Object
	receipt.ObservedObject = &workv1alpha1.ObservedObjectReference{
		UID:                observed.GetUID(),
		ResourceVersion:    observed.GetResourceVersion(),
		ObservedGeneration: observed.GetGeneration(),
	}
	receipt.Operation = mapOperation(outcome.Operation)
	receipt.Readback = outcome.Readback
	receipt.Phase = workv1alpha1.WorkApplyReceiptPhaseApplied
	if outcome.Readback {
		receipt.Reason = workv1alpha1.WorkApplyReceiptReasonObserved
	} else {
		receipt.Reason = workv1alpha1.WorkApplyReceiptReasonApplied
	}
	receipt.ConfirmedFields = c.confirmFields(result.workload, observed)
}

// confirmFields verifies observable aspects of the observed object against the desired manifest.
func (c *Controller) confirmFields(desired, observed *unstructured.Unstructured) []string {
	confirmed := []string{workv1alpha1.WorkApplyConfirmedIdentity}

	if util.GetLabelValue(observed.GetLabels(), util.ManagedByKarmadaLabel) == util.ManagedByKarmadaLabelValue &&
		observed.GetAnnotations()[workv1alpha2.WorkNamespaceAnnotation] == desired.GetAnnotations()[workv1alpha2.WorkNamespaceAnnotation] &&
		observed.GetAnnotations()[workv1alpha2.WorkNameAnnotation] == desired.GetAnnotations()[workv1alpha2.WorkNameAnnotation] {
		confirmed = append(confirmed, workv1alpha1.WorkApplyConfirmedOwnership)
	}
	if containsAll(observed.GetLabels(), desired.GetLabels()) {
		confirmed = append(confirmed, workv1alpha1.WorkApplyConfirmedLabels)
	}
	if containsAll(observed.GetAnnotations(), desired.GetAnnotations()) {
		confirmed = append(confirmed, workv1alpha1.WorkApplyConfirmedAnnotations)
	}

	observedPruned := observed.DeepCopy()
	if err := prune.RemoveIrrelevantFields(observedPruned, prune.RemoveJobTTLSeconds); err == nil {
		desiredDigest, err1 := applyreceipt.DigestForReceipt(desired)
		observedDigest, err2 := applyreceipt.DigestForReceipt(observedPruned)
		if err1 == nil && err2 == nil && desiredDigest == observedDigest {
			confirmed = append(confirmed, workv1alpha1.WorkApplyConfirmedContent)
		}
	}
	if len(confirmed) > workv1alpha1.MaxWorkApplyReceiptConfirmedFields {
		confirmed = confirmed[:workv1alpha1.MaxWorkApplyReceiptConfirmedFields]
	}
	return confirmed
}

// containsAll reports whether superset holds every key/value pair of subset.
func containsAll(superset, subset map[string]string) bool {
	for key, value := range subset {
		if superset[key] != value {
			return false
		}
	}
	return true
}

// fillUnknownReceipt maps an apply error to an explicit Unknown receipt.
func fillUnknownReceipt(receipt *workv1alpha1.WorkApplyReceipt, applyErr error) {
	receipt.Phase = workv1alpha1.WorkApplyReceiptPhaseUnknown
	switch {
	case stderrors.Is(applyErr, objectwatcher.ErrResourceConflict):
		receipt.Reason = workv1alpha1.WorkApplyReceiptReasonOwnershipConflict
	case apierrors.IsInternalError(applyErr) || isAmbiguousError(applyErr):
		receipt.Reason = workv1alpha1.WorkApplyReceiptReasonResultUnknown
	default:
		receipt.Reason = workv1alpha1.WorkApplyReceiptReasonApplyError
	}
	receipt.Message = applyreceipt.TruncateString(applyErr.Error(), workv1alpha1.MaxWorkApplyReceiptMessageLength)
}

// isAmbiguousError recognizes transport-level errors whose write result cannot be determined
// from the error alone (timeouts, lost connections).
func isAmbiguousError(err error) bool {
	var reasonErr apierrors.APIStatus
	if stderrors.As(err, &reasonErr) {
		reason := reasonErr.Status().Reason
		return reason == metav1.StatusReasonTimeout ||
			reason == metav1.StatusReasonServiceUnavailable ||
			reason == metav1.StatusReasonTooManyRequests
	}
	return false
}

// mapOperation converts ObjectWatcher operation results into receipt operation enums.
func mapOperation(result objectwatcher.OperationResult) workv1alpha1.WorkApplyOperation {
	switch result {
	case objectwatcher.OperationResultCreated:
		return workv1alpha1.WorkApplyOperationCreated
	case objectwatcher.OperationResultUpdated:
		return workv1alpha1.WorkApplyOperationUpdated
	case objectwatcher.OperationResultUnchanged:
		return workv1alpha1.WorkApplyOperationUnchanged
	default:
		return workv1alpha1.WorkApplyOperationObserved
	}
}

// extractMutationSummary reads the webhook-produced mutation summary annotation.
// Works created before the feature carry no such annotation; this is recorded explicitly.
func extractMutationSummary(workload *unstructured.Unstructured) *workv1alpha1.ManifestMutationSummary {
	raw := workload.GetAnnotations()[workv1alpha2.WorkMutationSummaryAnnotation]
	if raw == "" {
		return &workv1alpha1.ManifestMutationSummary{WebhookSummaryUnavailable: true}
	}
	summary := &workv1alpha1.ManifestMutationSummary{}
	if err := json.Unmarshal([]byte(raw), summary); err != nil {
		return &workv1alpha1.ManifestMutationSummary{WebhookSummaryUnavailable: true}
	}
	return summary
}
