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

package shadow

import (
	"github.com/prometheus/client_golang/prometheus"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
)

const (
	shadowCounterMetricsName  = "shadow_resource_interpreter_operation_total"
	shadowDurationMetricsName = "shadow_resource_interpreter_duration_seconds"
)

// Labels are deliberately restricted to operation, API group, kind and an enum
// outcome. Resource names, UIDs and script versions are excluded so that label
// cardinality stays bounded regardless of workload volume.
var (
	shadowOperationCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: shadowCounterMetricsName,
		Help: "Total number of shadow resource interpreter evaluations partitioned by operation, resource type and bounded outcome.",
	}, []string{"operation", "group", "kind", "outcome"})

	shadowDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    shadowDurationMetricsName,
		Help:    "Candidate script execution duration in seconds for shadow resource interpreter evaluations.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0},
	}, []string{"operation", "group", "kind"})
)

// RecordShadowEvaluation increments the outcome counter for one evaluation.
func RecordShadowEvaluation(operation configv1alpha1.InterpreterOperation, group, kind string, outcome Outcome) {
	shadowOperationCounter.WithLabelValues(string(operation), group, kind, string(outcome)).Inc()
}

// RecordShadowDuration observes the candidate execution duration for one evaluation.
func RecordShadowDuration(operation configv1alpha1.InterpreterOperation, group, kind string, durationSeconds float64) {
	shadowDurationHistogram.WithLabelValues(string(operation), group, kind).Observe(durationSeconds)
}

// Collectors returns the prometheus collectors owned by the shadow interpreter.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{shadowOperationCounter, shadowDurationHistogram}
}
