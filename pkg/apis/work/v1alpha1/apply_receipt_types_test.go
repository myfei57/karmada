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

package v1alpha1

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestWorkApplyReceiptDeepCopy(t *testing.T) {
	original := &WorkApplyReceipt{
		Identifier:       ResourceIdentifier{Ordinal: 1, Group: "apps", Version: "v1", Kind: "Deployment", Resource: "deployments", Namespace: "ns", Name: "demo"},
		WorkUID:          types.UID("work-uid"),
		WorkGeneration:   2,
		ClusterUID:       types.UID("cluster-uid"),
		ClusterID:        "member-cluster-id",
		ManifestIdentity: "apps/v1,Kind=Deployment,ns/demo",
		RequestDigest:    "sha256:request",
		AppliedDigest:    "sha256:applied",
		MutationSummary: &ManifestMutationSummary{
			AddedLabels:               []string{"label-a"},
			AddedAnnotations:          []string{"annotation-a"},
			RemovedFields:             []string{"metadata.uid"},
			WebhookSummaryUnavailable: false,
		},
		ObservedObject: &ObservedObjectReference{
			UID:                types.UID("object-uid"),
			ResourceVersion:    "12345",
			ObservedGeneration: 3,
		},
		Operation:       WorkApplyOperationUpdated,
		Readback:        true,
		ConfirmedFields: []string{WorkApplyConfirmedIdentity, WorkApplyConfirmedOwnership},
		SecretRedacted:  true,
		Phase:           WorkApplyReceiptPhaseApplied,
		Reason:          WorkApplyReceiptReasonObserved,
		Message:         "observed via readback",
		ObservedAt:      metav1.Now(),
	}

	cloned := original.DeepCopy()
	if !reflect.DeepEqual(original, cloned) {
		t.Fatalf("DeepCopy() result differs from original")
	}

	// Mutate the clone and ensure the shared slices and pointers stay independent.
	cloned.ConfirmedFields[0] = "tampered"
	cloned.MutationSummary.AddedLabels[0] = "tampered"
	cloned.ObservedObject.ResourceVersion = "999"
	if original.ConfirmedFields[0] == "tampered" {
		t.Fatal("DeepCopy() shares ConfirmedFields backing array")
	}
	if original.MutationSummary.AddedLabels[0] == "tampered" {
		t.Fatal("DeepCopy() shares MutationSummary backing array")
	}
	if original.ObservedObject.ResourceVersion == "999" {
		t.Fatal("DeepCopy() shares ObservedObject pointer")
	}
}

func TestWorkStatusDeepCopyWithReceipts(t *testing.T) {
	status := &WorkStatus{
		WorkApplyReceipts: []WorkApplyReceipt{
			{
				Identifier:       ResourceIdentifier{Ordinal: 0, Version: "v1", Kind: "Secret", Resource: "secrets", Name: "s"},
				WorkUID:          types.UID("w"),
				WorkGeneration:   1,
				ClusterUID:       types.UID("c"),
				AppliedDigest:    "sha256:x",
				Operation:        WorkApplyOperationCreated,
				Phase:            WorkApplyReceiptPhaseUnknown,
				Reason:           WorkApplyReceiptReasonResultUnknown,
				SecretRedacted:   true,
				ObservedAt:       metav1.Now(),
				ManifestIdentity: "v1,Kind=Secret,/s",
			},
		},
	}

	cloned := status.DeepCopy()
	if !reflect.DeepEqual(status, cloned) {
		t.Fatalf("WorkStatus.DeepCopy() result differs from original")
	}
	cloned.WorkApplyReceipts[0].Phase = WorkApplyReceiptPhaseApplied
	if status.WorkApplyReceipts[0].Phase == WorkApplyReceiptPhaseApplied {
		t.Fatal("WorkStatus.DeepCopy() shares WorkApplyReceipts backing array")
	}
}
