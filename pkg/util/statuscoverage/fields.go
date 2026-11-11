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
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"

	"github.com/karmada-io/karmada/pkg/util"
)

// upperBoundSource describes where a trustworthy per-cluster upper bound for
// a field comes from. Fields without a bound source can only report an Unknown
// upper bound while coverage is incomplete.
type upperBoundSource string

const (
	// boundNone means no trustworthy upper bound is available for the field.
	boundNone upperBoundSource = ""
	// boundDesiredReplicas means the cluster's desired replica assignment is
	// the upper bound (only meaningful when it is positive).
	boundDesiredReplicas upperBoundSource = "DesiredReplicas"
	// boundAllocatedCompletions means the cluster's allocated Job
	// completions is the upper bound.
	boundAllocatedCompletions upperBoundSource = "AllocatedCompletions"
)

// fieldSpec describes one coverable status field.
type fieldSpec struct {
	// name is the JSON field name inside the reflected status payload.
	name string
	// bound is the per-cluster upper bound source used for partial totals.
	bound upperBoundSource
}

// reflectedDeploymentStatus mirrors the payload produced by the default
// Deployment reflect status interpreter (WrappedDeploymentStatus). It is
// duplicated here to keep the coverage protocol decoupled from the
// interpreter package; the JSON wire format must stay identical.
type reflectedDeploymentStatus struct {
	Generation                 int64 `json:"generation,omitempty"`
	ResourceTemplateGeneration int64 `json:"resourceTemplateGeneration,omitempty"`
	appsv1.DeploymentStatus    `json:",inline"`
}

// reflectedJobStatus mirrors the payload produced by the default Job reflect
// status interpreter.
type reflectedJobStatus struct {
	batchv1.JobStatus `json:",inline"`
}

// deploymentFields are the Deployment status fields covered by the report.
// Only ready/available replicas admit a desired-replica-based upper bound:
// current/updated/unavailable replicas can transiently exceed the desired
// assignment during rollouts, so no trustworthy bound exists while coverage
// is incomplete.
var deploymentFields = []fieldSpec{
	{name: util.ReplicasField, bound: boundNone},
	{name: util.ReadyReplicasField, bound: boundDesiredReplicas},
	{name: "availableReplicas", bound: boundDesiredReplicas},
	{name: "updatedReplicas", bound: boundNone},
	{name: "unavailableReplicas", bound: boundNone},
}

// jobFields are the Job status fields covered by the report. Only succeeded
// pods are bounded, by the per-cluster allocated completions. Active and
// failed counts have no trustworthy upper bound.
var jobFields = []fieldSpec{
	{name: "active", bound: boundNone},
	{name: "succeeded", bound: boundAllocatedCompletions},
	{name: "failed", bound: boundNone},
}

// fieldSpecsFor returns the registered field specs for a kind. The second
// return value is false when the kind has no coverage field registry.
func fieldSpecsFor(kind string) ([]fieldSpec, bool) {
	switch kind {
	case util.DeploymentKind:
		return deploymentFields, true
	case util.JobKind:
		return jobFields, true
	default:
		return nil, false
	}
}

// fieldBound returns the trustworthy per-cluster upper bound for a field on a
// target. The second return value is false when no trustworthy bound exists.
func fieldBound(spec fieldSpec, target Target) (int64, bool) {
	switch spec.bound {
	case boundDesiredReplicas:
		if target.DesiredReplicas <= 0 {
			return 0, false
		}
		return int64(target.DesiredReplicas), true
	case boundAllocatedCompletions:
		if target.AllocatedCompletions == nil || *target.AllocatedCompletions <= 0 {
			return 0, false
		}
		return int64(*target.AllocatedCompletions), true
	default:
		return 0, false
	}
}
