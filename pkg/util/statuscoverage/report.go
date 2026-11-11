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
	"encoding/json"
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/helper"
)

// Default output caps. They bound report size so a binding with a huge target
// set cannot produce an unbounded status payload.
const (
	DefaultMaxTargets       = 100
	DefaultMaxFields        = 20
	DefaultMaxErrors        = 50
	DefaultMaxReasonEntries = 20
	// maxAppliedMessageLength bounds the applied messages copied into the
	// report.
	maxAppliedMessageLength = 256
)

// StrictGateConfig enables the optional strict gate. It must be passed
// explicitly by the caller; the default aggregator never turns it on. The
// strict result is reported alongside, but kept separate from, the legacy
// health fields.
type StrictGateConfig struct {
	// EnforceAll requires every requested field to have a complete strict
	// total, otherwise the gate fails.
	EnforceAll bool
}

// Options controls report building.
type Options struct {
	// MaxTargets caps the per-cluster entries in the report. Zero falls back
	// to DefaultMaxTargets.
	MaxTargets int
	// MaxFields caps the per-field summaries in the report. Zero falls back
	// to DefaultMaxFields.
	MaxFields int
	// MaxErrors caps the integrity error strings in the report. Zero falls
	// back to DefaultMaxErrors.
	MaxErrors int
	// MaxReasonEntries caps the cluster-name lists attached to each field
	// summary. Zero falls back to DefaultMaxReasonEntries.
	MaxReasonEntries int

	// RequestedFields restricts the reported fields. A registered field that
	// is not in this list is reported as NotRequested. nil means every
	// registered field is requested.
	// +optional
	RequestedFields []string

	// StrictGate, when non-nil, enables the explicit strict gate. nil means
	// the gate is not configured and stays disabled.
	// +optional
	StrictGate *StrictGateConfig
}

// AllocateJobCompletions re-derives the per-cluster Job completions from the
// current target set using the same division algorithm as the binding
// controller, so allocations always match the current binding and succeeded
// counts from removed targets can never enter the new total.
//
// templateCompletions is spec.completions of the Job template. When it is not
// set, no allocation is produced and the returned targets keep nil
// AllocatedCompletions.
func AllocateJobCompletions(templateCompletions int64, targets []Target, templateUID types.UID) []Target {
	result := make([]Target, len(targets))
	copy(result, targets)
	if templateCompletions <= 0 {
		return result
	}

	tcs := make([]workv1alpha2.TargetCluster, 0, len(targets))
	for _, t := range targets {
		// The per-cluster replica assignment acts as the division weight,
		// exactly as in the binding controller's Job completions division.
		tcs = append(tcs, workv1alpha2.TargetCluster{Name: t.Identity.Name, Replicas: t.DesiredReplicas})
	}
	allocated := helper.SpreadReplicasByTargetClusters(int32(templateCompletions), tcs, nil, templateUID) // #nosec G115: integer overflow conversion int64 -> int32
	allocationMap := make(map[string]int32, len(allocated))
	for _, tc := range allocated {
		allocationMap[tc.Name] = tc.Replicas
	}
	for i := range result {
		if replicas, ok := allocationMap[result[i].Identity.Name]; ok {
			replicasCopy := replicas
			result[i].AllocatedCompletions = &replicasCopy
		}
	}
	return result
}

// DuplicatedJobCompletions assigns the full template completions to every
// current target, matching duplicated Job scheduling where each member
// cluster runs an independent, complete Job. When templateCompletions is not
// positive no allocation is produced.
func DuplicatedJobCompletions(templateCompletions int64, targets []Target) []Target {
	result := make([]Target, len(targets))
	copy(result, targets)
	if templateCompletions <= 0 {
		return result
	}
	for i := range result {
		completions := int32(templateCompletions) // #nosec G115: integer overflow conversion int64 -> int32
		result[i].AllocatedCompletions = &completions
	}
	return result
}

// SameMemberResourceVersion compares two opaque member object
// resourceVersions using string equality only. Resource versions are opaque
// values: callers must never parse them as integers, compare them
// numerically, or order clusters by them.
func SameMemberResourceVersion(a, b string) bool {
	return a == b
}

// Build constructs a read-only StatusCoverageReport.
//
// The target universe is taken exclusively from targets; it is never inferred
// from evidences. clusters provides reachability and cluster UID evidence.
// The function is pure: it does not read or modify any binding, work or
// resource object, and it refuses Secret templates whose status paths must
// never be read.
func Build(
	info ResourceTemplateInfo,
	targets []Target,
	clusters []clusterv1alpha1.Cluster,
	evidences []Evidence,
	opts Options,
) (*StatusCoverageReport, error) {
	if info.Kind == util.SecretKind {
		return nil, fmt.Errorf("status coverage is not allowed for Secret %s/%s: status paths are never read", info.Namespace, info.Name)
	}

	specs, registered := fieldSpecsFor(info.Kind)
	if !registered {
		return nil, fmt.Errorf("status coverage is not supported for kind %q", info.Kind)
	}

	opts = opts.withDefaults()
	requested, err := resolveRequestedFields(specs, opts.RequestedFields)
	if err != nil {
		return nil, err
	}

	report := &StatusCoverageReport{
		Resource:    info,
		GeneratedAt: metav1.Now(),
		Targets:     len(targets),
	}

	clusterMap := make(map[string]clusterv1alpha1.Cluster, len(clusters))
	for _, c := range clusters {
		clusterMap[c.Name] = c
	}

	evidenceMap := make(map[string]Evidence, len(evidences))
	targetNames := sets.New[string]()
	for _, t := range targets {
		targetNames.Insert(t.Identity.Name)
	}
	for _, ev := range evidences {
		if !targetNames.Has(ev.ClusterName) {
			// Evidence from a cluster that is no longer targeted (for
			// example after a placement removal) is extraneous and never
			// aggregated.
			report.ExtraneousClusters = appendCapped(report.ExtraneousClusters, ev.ClusterName, opts.MaxReasonEntries, &report.Truncated)
			continue
		}
		if _, exists := evidenceMap[ev.ClusterName]; exists {
			report.appendError(fmt.Sprintf("duplicate status evidence for cluster %q, keeping the first one", ev.ClusterName), opts)
			continue
		}
		evidenceMap[ev.ClusterName] = ev
	}

	sortedTargets := make([]Target, len(targets))
	copy(sortedTargets, targets)
	sort.Slice(sortedTargets, func(i, j int) bool {
		return sortedTargets[i].Identity.Name < sortedTargets[j].Identity.Name
	})

	// perField holds cluster states keyed by field name for total
	// computation.
	perField := make(map[string][]fieldClusterState, len(specs))
	for _, spec := range specs {
		perField[spec.name] = make([]fieldClusterState, 0, len(sortedTargets))
	}

	for _, target := range sortedTargets {
		coverage := buildClusterCoverage(info, target, clusterMap, evidenceMap, specs, requested, opts, report)
		if len(report.Clusters) < opts.MaxTargets {
			report.Clusters = append(report.Clusters, coverage)
		} else {
			report.Truncated = true
		}

		for _, spec := range specs {
			fc := coverage.Fields[spec.name]
			perField[spec.name] = append(perField[spec.name], fieldClusterState{
				cluster:  target.Identity.Name,
				coverage: fc,
				target:   target,
				spec:     spec,
			})
		}
	}

	for _, spec := range specs {
		if !requested.Has(spec.name) {
			continue
		}
		summary := summarizeField(spec, perField[spec.name], opts)
		if len(report.Fields) < opts.MaxFields {
			report.Fields = append(report.Fields, summary)
		} else {
			report.Truncated = true
		}
	}

	report.StrictGate = evaluateStrictGate(opts, report.Fields)
	return report, nil
}

// fieldClusterState bundles everything summarizeField needs for one cluster.
type fieldClusterState struct {
	cluster  string
	coverage FieldCoverage
	target   Target
	spec     fieldSpec
}

func (o Options) withDefaults() Options {
	if o.MaxTargets <= 0 {
		o.MaxTargets = DefaultMaxTargets
	}
	if o.MaxFields <= 0 {
		o.MaxFields = DefaultMaxFields
	}
	if o.MaxErrors <= 0 {
		o.MaxErrors = DefaultMaxErrors
	}
	if o.MaxReasonEntries <= 0 {
		o.MaxReasonEntries = DefaultMaxReasonEntries
	}
	return o
}

func resolveRequestedFields(specs []fieldSpec, requested []string) (sets.Set[string], error) {
	known := sets.New[string]()
	for _, spec := range specs {
		known.Insert(spec.name)
	}
	if len(requested) == 0 {
		return known, nil
	}
	selected := sets.New[string]()
	for _, name := range requested {
		if !known.Has(name) {
			return nil, fmt.Errorf("requested field %q is not registered for the resource kind", name)
		}
		selected.Insert(name)
	}
	return selected, nil
}

func buildClusterCoverage(
	info ResourceTemplateInfo,
	target Target,
	clusterMap map[string]clusterv1alpha1.Cluster,
	evidenceMap map[string]Evidence,
	specs []fieldSpec,
	requested sets.Set[string],
	opts Options,
	report *StatusCoverageReport,
) ClusterCoverage {
	coverage := ClusterCoverage{
		Cluster: target.Identity,
		Fields:  make(map[string]FieldCoverage, len(specs)),
	}

	cluster, clusterFound := clusterMap[target.Identity.Name]
	coverage.Reachable = clusterFound && util.IsClusterReady(&cluster.Status)

	// Cluster-level failures first: every field inherits the same state.
	if fc, failed := clusterReachabilityFailure(clusterFound, coverage.Reachable); failed {
		coverage.fillAll(specs, requested, fc)
		return coverage
	}
	if fc, failed := clusterIdentityFailure(target, cluster, report, opts); failed {
		coverage.fillAll(specs, requested, fc)
		return coverage
	}

	ev, hasEvidence := evidenceMap[target.Identity.Name]
	if !hasEvidence {
		coverage.fillAll(specs, requested, FieldCoverage{State: FieldStateMissing, Reason: ReasonNoEvidence,
			Detail: "no AggregatedStatusItem returned for the target cluster"})
		return coverage
	}

	// Evidence integrity: the evidence must come from the current cluster
	// instance and, when an expected member UID is known, from the current
	// member object.
	if fc, failed := evidenceIdentityFailure(target, cluster, ev, report, opts); failed {
		coverage.fillAll(specs, requested, fc)
		return coverage
	}
	coverage.IdentityVerified = true
	memberIdentity := ev.Member
	coverage.MemberObject = &memberIdentity

	if fc, failed := reflectedStatusFailure(ev); failed {
		coverage.fillAll(specs, requested, fc)
		return coverage
	}

	values, stale, parseErr := parseEvidence(info, ev.Item.StatusRaw)
	if parseErr != nil {
		detail := "status payload cannot be parsed: " + truncate(parseErr.Error(), maxAppliedMessageLength)
		report.appendError(fmt.Sprintf("cluster %q: %s", target.Identity.Name, detail), opts)
		coverage.fillAll(specs, requested, FieldCoverage{State: FieldStateInterpreterError, Reason: ReasonInterpretError, Detail: detail})
		return coverage
	}
	if stale {
		coverage.fillAll(specs, requested, FieldCoverage{
			State:                      FieldStateStaleGeneration,
			Reason:                     ReasonStaleGeneration,
			Detail:                     "observed status corresponds to an older resource template generation",
			ObservedTemplateGeneration: values.observedTemplateGeneration,
		})
		return coverage
	}

	applyValidFields(&coverage, specs, requested, values)
	return coverage
}

// clusterReachabilityFailure reports an unreachable cluster.
func clusterReachabilityFailure(clusterFound, reachable bool) (FieldCoverage, bool) {
	if !clusterFound {
		return FieldCoverage{State: FieldStateClusterUnreachable, Reason: ReasonClusterNotFound,
			Detail: "cluster object not found"}, true
	}
	if !reachable {
		return FieldCoverage{State: FieldStateClusterUnreachable, Reason: ReasonClusterNotReady,
			Detail: "cluster Ready condition is not True"}, true
	}
	return FieldCoverage{}, false
}

// clusterIdentityFailure guards against a rebuilt same-name cluster.
func clusterIdentityFailure(target Target, cluster clusterv1alpha1.Cluster, report *StatusCoverageReport, opts Options) (FieldCoverage, bool) {
	if target.Identity.UID == "" || cluster.UID == target.Identity.UID {
		return FieldCoverage{}, false
	}
	detail := fmt.Sprintf("cluster UID mismatch: target expects %q but current cluster is %q (same-name rebuild)",
		target.Identity.UID, cluster.UID)
	report.appendError(fmt.Sprintf("cluster %q: %s", target.Identity.Name, detail), opts)
	return FieldCoverage{State: FieldStateMissing, Reason: ReasonClusterUIDMismatch, Detail: detail}, true
}

// evidenceIdentityFailure guards against evidence produced by an old cluster
// instance or by a rebuilt same-name member object.
func evidenceIdentityFailure(target Target, cluster clusterv1alpha1.Cluster, ev Evidence, report *StatusCoverageReport, opts Options) (FieldCoverage, bool) {
	if ev.ClusterUID != "" && ev.ClusterUID != cluster.UID {
		detail := fmt.Sprintf("evidence cluster UID %q does not match current cluster UID %q (same-name rebuild)",
			ev.ClusterUID, cluster.UID)
		report.appendError(fmt.Sprintf("cluster %q: %s", target.Identity.Name, detail), opts)
		return FieldCoverage{State: FieldStateMissing, Reason: ReasonClusterUIDMismatch, Detail: detail}, true
	}
	if target.ExpectedMemberUID != "" && ev.Member.UID != target.ExpectedMemberUID {
		detail := fmt.Sprintf("member object UID mismatch: expected %q but observed %q (same-name rebuild)",
			target.ExpectedMemberUID, ev.Member.UID)
		report.appendError(fmt.Sprintf("cluster %q: %s", target.Identity.Name, detail), opts)
		return FieldCoverage{State: FieldStateMissing, Reason: ReasonMemberObjectUIDMismatch, Detail: detail}, true
	}
	return FieldCoverage{}, false
}

// reflectedStatusFailure reports missing reflected status payloads.
func reflectedStatusFailure(ev Evidence) (FieldCoverage, bool) {
	if ev.Item != nil && ev.Item.Applied && len(ev.Item.StatusRaw) > 0 {
		return FieldCoverage{}, false
	}
	detail := "no reflected status available"
	if ev.Item != nil && len(ev.Item.AppliedMessage) > 0 {
		detail = "no reflected status available: " + truncate(ev.Item.AppliedMessage, maxAppliedMessageLength)
	}
	return FieldCoverage{State: FieldStateMissing, Reason: ReasonNoReflectedStatus, Detail: detail}, true
}

// applyValidFields records parseable, current field values. An absent numeric
// field in a parseable payload means the member reports the zero value.
func applyValidFields(coverage *ClusterCoverage, specs []fieldSpec, requested sets.Set[string], values parsedEvidence) {
	for _, spec := range specs {
		if !requested.Has(spec.name) {
			coverage.Fields[spec.name] = FieldCoverage{State: FieldStateNotRequested, Reason: ReasonNotRequested,
				Detail: "field is explicitly not requested"}
			continue
		}
		value, ok := values.values[spec.name]
		if !ok {
			value = 0
		}
		state := FieldStateValid
		if value == 0 {
			state = FieldStateValidZero
		}
		coverage.Fields[spec.name] = FieldCoverage{
			State:                      state,
			Value:                      &value,
			ObservedTemplateGeneration: values.observedTemplateGeneration,
		}
	}
}

func (c *ClusterCoverage) fillAll(specs []fieldSpec, requested sets.Set[string], fc FieldCoverage) {
	for _, spec := range specs {
		if requested.Has(spec.name) {
			c.Fields[spec.name] = fc
		} else {
			c.Fields[spec.name] = FieldCoverage{State: FieldStateNotRequested, Reason: ReasonNotRequested,
				Detail: "field is explicitly not requested"}
		}
	}
}

// parsedEvidence holds the numeric values extracted from a status payload.
type parsedEvidence struct {
	values                     map[string]int64
	observedTemplateGeneration int64
}

// parseEvidence parses the reflected status payload and reports staleness.
// Staleness is only detectable for kinds whose reflected payload carries
// generation evidence (Deployment); other kinds cannot be flagged stale from
// the payload and rely on the identity and target checks above.
func parseEvidence(info ResourceTemplateInfo, raw []byte) (parsedEvidence, bool, error) {
	switch info.Kind {
	case util.DeploymentKind:
		member := &reflectedDeploymentStatus{}
		if err := json.Unmarshal(raw, member); err != nil {
			return parsedEvidence{}, false, err
		}
		stale := !(member.ObservedGeneration >= member.Generation &&
			member.ResourceTemplateGeneration >= info.Generation)
		return parsedEvidence{
			values: map[string]int64{
				util.ReplicasField:      int64(member.Replicas),
				util.ReadyReplicasField: int64(member.ReadyReplicas),
				"availableReplicas":     int64(member.AvailableReplicas),
				"updatedReplicas":       int64(member.UpdatedReplicas),
				"unavailableReplicas":   int64(member.UnavailableReplicas),
			},
			observedTemplateGeneration: member.ResourceTemplateGeneration,
		}, stale, nil
	case util.JobKind:
		member := &reflectedJobStatus{}
		if err := json.Unmarshal(raw, member); err != nil {
			return parsedEvidence{}, false, err
		}
		return parsedEvidence{
			values: map[string]int64{
				"active":    int64(member.Active),
				"succeeded": int64(member.Succeeded),
				"failed":    int64(member.Failed),
			},
		}, false, nil
	default:
		return parsedEvidence{}, false, fmt.Errorf("unsupported kind %q", info.Kind)
	}
}

func summarizeField(spec fieldSpec, states []fieldClusterState, opts Options) FieldCoverageSummary {
	summary := FieldCoverageSummary{Field: spec.name}
	var lower int64
	var upper int64
	upperKnown := true
	unknown := false

	for _, st := range states {
		switch st.coverage.State {
		case FieldStateValid, FieldStateValidZero:
			lower += *st.coverage.Value
			if st.coverage.State == FieldStateValidZero {
				summary.ValidZeroClusters++
			}
			summary.CoveredClusters++
		default:
			unknown = true
			switch st.coverage.State {
			case FieldStateMissing:
				summary.MissingClusters = appendCapped(summary.MissingClusters, st.cluster, opts.MaxReasonEntries, nil)
			case FieldStateStaleGeneration:
				summary.StaleClusters = appendCapped(summary.StaleClusters, st.cluster, opts.MaxReasonEntries, nil)
			case FieldStateClusterUnreachable:
				summary.UnreachableClusters = appendCapped(summary.UnreachableClusters, st.cluster, opts.MaxReasonEntries, nil)
			case FieldStateInterpreterError:
				summary.InterpreterErrorClusters = appendCapped(summary.InterpreterErrorClusters, st.cluster, opts.MaxReasonEntries, nil)
			}
			bound, ok := fieldBound(st.spec, st.target)
			if ok {
				upper += bound
			} else {
				upperKnown = false
			}
		}
	}

	summary.Partial.Unknown = unknown
	summary.Partial.Lower = lower
	if !unknown {
		// Full coverage: the partial view collapses to the exact total.
		exact := lower
		summary.Strict = StrictTotal{Complete: len(states) > 0, Value: &exact}
		summary.Partial.Upper = &exact
		return summary
	}

	// Incomplete coverage: the strict total is Unknown and must never be
	// faked as zero or as a complete success.
	if upperKnown {
		bounded := lower + upper
		summary.Partial.Upper = &bounded
	}
	return summary
}

func evaluateStrictGate(opts Options, summaries []FieldCoverageSummary) StrictGateState {
	if opts.StrictGate == nil {
		return StrictGateState{Enabled: false, Reason: "NotConfigured"}
	}
	if !features.FeatureGate.Enabled(features.StatusCoverageReport) {
		return StrictGateState{Enabled: false, Reason: "FeatureGateDisabled:StatusCoverageReport"}
	}

	if !opts.StrictGate.EnforceAll {
		// Audit mode: the gate is explicitly enabled but never fails, so
		// callers can consume the coverage report without enforcement.
		return StrictGateState{Enabled: true, Pass: true, Reason: "AuditMode"}
	}

	state := StrictGateState{Enabled: true, Pass: true}
	for _, summary := range summaries {
		if !summary.Strict.Complete {
			state.Pass = false
			state.FailedFields = appendCapped(state.FailedFields, summary.Field, opts.MaxReasonEntries, nil)
		}
	}
	if !state.Pass {
		state.Reason = "IncompleteCoverage"
	}
	return state
}

func (r *StatusCoverageReport) appendError(msg string, opts Options) {
	if len(r.Errors) < opts.MaxErrors {
		r.Errors = append(r.Errors, msg)
	} else {
		r.Truncated = true
	}
}

func appendCapped(list []string, value string, limit int, truncated *bool) []string {
	if len(list) < limit {
		return append(list, value)
	}
	if truncated != nil {
		*truncated = true
	}
	return list
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}
