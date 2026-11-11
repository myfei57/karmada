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

// Package affinity synthesizes ClusterAffinity expressions that precisely
// select a requested set of member clusters over an explicitly authorized
// cluster snapshot.
//
// The synthesizer is a pure, side-effect-free library: it never creates,
// updates or deletes policies, clusters or labels, and it only observes the
// snapshot handed to it. Clusters are identified exclusively by their
// metadata.uid so that a deleted and re-created cluster reusing the same name
// is treated as a different entity. Synthesized expressions only contain
// label requirements, therefore cluster names of the authorized inventory
// never leak into the result.
//
// All guarantees are scoped to the snapshot identified in the result. Clusters
// that join the inventory after the snapshot was taken are outside the
// guarantee scope.
package affinity

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"github.com/karmada-io/karmada/pkg/util"
)

const (
	// MaxRequirements is the fixed grammar limit: a synthesized affinity may
	// declare at most this many MatchExpressions.
	MaxRequirements = 8

	// DefaultSearchNodeBudget is the default maximum number of branch-and-bound
	// nodes evaluated for one suggestion. Exhausting the budget yields an
	// Incomplete result instead of an unproven global-minimum claim.
	DefaultSearchNodeBudget int64 = 20000

	// maxCandidateRequirements bounds the size of the finite candidate family.
	// The family grows exponentially with the number of distinct allowed values
	// per key; when it exceeds this cap the request cannot be searched honestly
	// and an Incomplete result is returned.
	maxCandidateRequirements = 256

	// snapshotIDHexLength is the number of hex characters retained in the
	// snapshot identity hash.
	snapshotIDHexLength = 16

	// canonical separators use control characters that are illegal in label
	// keys and values, so canonical strings cannot collide.
	canonicalFieldSep = "\x1f"
	canonicalEntrySep = "\x1e"

	// unmentionableToken marks a key that is present with a value outside the
	// allowed value vocabulary, which no In/NotIn requirement can mention.
	unmentionableToken = "\x00"
)

// ErrInvalidRequest is returned when a SuggestClusterAffinityRequest is
// malformed. Callers can use errors.Is to detect validation failures.
var ErrInvalidRequest = errors.New("invalid SuggestClusterAffinity request")

// Status describes the guarantee level carried by a suggestion.
type Status string

const (
	// StatusOptimal means the branch-and-bound search completed within budget
	// and the returned expression uses the proven minimum number of
	// requirements within the fixed grammar.
	StatusOptimal Status = "Optimal"

	// StatusIncomplete means a finite search budget was exhausted. The result,
	// when present, is exact on the snapshot but only bounded by the proven
	// lower and upper bounds; global minimality is not claimed.
	StatusIncomplete Status = "Incomplete"

	// StatusInfeasible means no expression allowed by the fixed grammar,
	// vocabulary and maximum item count can separate every target cluster from
	// every non-target cluster on the snapshot.
	StatusInfeasible Status = "Infeasible"
)

// Vocabulary is the closed allow-list of label keys and values that the
// synthesizer may use. Labels outside the vocabulary are never observed for
// expression synthesis and cannot cause the vocabulary to grow.
type Vocabulary struct {
	// AllowedValues maps an allowed label key to its allowed label values.
	// Only keys present in this map may be used. A key mapped to an empty slice
	// only permits existence-based requirements (Exists/DoesNotExist); its
	// values observed on clusters are then treated as unmentionable.
	AllowedValues map[string][]string
}

// SuggestClusterAffinityRequest carries the authorized snapshot, the target
// cluster UID set and the closed label vocabulary used for synthesis.
type SuggestClusterAffinityRequest struct {
	// Snapshot is the explicitly authorized cluster inventory. It is the only
	// inventory observable by the synthesizer.
	Snapshot []*clusterv1alpha1.Cluster

	// TargetUIDs is the exact set of cluster UIDs the synthesized affinity must
	// select on the snapshot. Every UID must exist in Snapshot.
	TargetUIDs []types.UID

	// Vocabulary restricts the label keys and values that may appear in the
	// synthesized expression.
	Vocabulary Vocabulary

	// MaxRequirements overrides the per-request item cap. It must be positive
	// and cannot exceed the fixed MaxRequirements constant; zero selects the
	// fixed default.
	MaxRequirements int

	// SearchNodeBudget overrides the branch-and-bound node budget. Zero selects
	// DefaultSearchNodeBudget.
	SearchNodeBudget int64
}

// SnapshotIdentity stably identifies the authorized snapshot a suggestion was
// computed against. The guarantee only holds for this exact inventory.
type SnapshotIdentity struct {
	// ID is a stable hash of the sorted snapshot UIDs.
	ID string

	// ClusterCount is the number of clusters contained in the snapshot.
	ClusterCount int

	// UIDs are the snapshot cluster UIDs in stable sorted order. Cluster names
	// are intentionally absent.
	UIDs []types.UID
}

// IndistinguishablePair witnesses a target and a non-target cluster that no
// vocabulary-restricted requirement can distinguish: both project onto the
// same presence/value signature for every allowed key.
type IndistinguishablePair struct {
	// TargetUID is the UID of a cluster that must be selected.
	TargetUID types.UID

	// NonTargetUID is the UID of a cluster that must not be selected but
	// cannot be separated from TargetUID within the vocabulary.
	NonTargetUID types.UID
}

// SuggestClusterAffinityResult is the synthesis outcome. When Status is
// Infeasible, Affinity is nil and Witnesses may explain the impossibility.
type SuggestClusterAffinityResult struct {
	// Status is the guarantee level of this result.
	Status Status

	// Affinity is the synthesized ClusterAffinity. It only carries a
	// LabelSelector; cluster names and field selectors are never used. It is
	// nil on an Infeasible result.
	Affinity *policyv1alpha1.ClusterAffinity

	// Snapshot identifies the inventory this result was computed against.
	Snapshot SnapshotIdentity

	// TargetUIDs is the requested target set in stable sorted order.
	TargetUIDs []types.UID

	// MatchedUIDs is the UID set selected by Affinity according to the real
	// ClusterMatches predicate, in stable sorted order. It equals TargetUIDs
	// whenever Affinity is non-nil.
	MatchedUIDs []types.UID

	// RequirementCount is the number of declared MatchExpressions.
	RequirementCount int

	// ProvenLowerBound is a proven lower bound on the minimum achievable
	// requirement count. It equals RequirementCount for an Optimal result.
	ProvenLowerBound int

	// ProvenUpperBound is the proven upper bound given by Affinity.
	ProvenUpperBound int

	// Witnesses lists target/non-target pairs that cannot be distinguished
	// within the vocabulary. It is populated for Infeasible results.
	Witnesses []IndistinguishablePair

	// NodesEvaluated is the number of branch-and-bound nodes consumed.
	NodesEvaluated int64

	// BudgetExhausted reports whether the search stopped because the node
	// budget was reached.
	BudgetExhausted bool

	// Message is a human-readable, name-free explanation of the status.
	Message string
}

// sigToken is the projection of one allowed label key on a cluster. A missing
// key is represented explicitly (present=false) and is never confused with an
// empty string value.
type sigToken struct {
	// present reports whether the cluster carries the key.
	present bool

	// allowed reports whether a present value belongs to the allowed value
	// vocabulary. Unmentionable values collapse to a single equivalence class.
	allowed bool

	// value is the actual label value when present and allowed, otherwise "".
	value string
}

// candidate is one finite label requirement together with the set of
// non-target clusters (identified by UID-sorted positions) it rejects.
type candidate struct {
	expr      metav1.LabelSelectorRequirement
	canonical string
	kill      sets.Set[int]
}

// normalizedRequest holds the validated, deterministically ordered inputs.
type normalizedRequest struct {
	clusters     []*clusterv1alpha1.Cluster
	uids         []types.UID
	targets      sets.Set[types.UID]
	vocab        map[string]sets.Set[string]
	maxReq       int
	searchBudget int64
}

// SuggestClusterAffinity synthesizes a finite ClusterAffinity that selects
// exactly the requested target UIDs on the authorized snapshot, while
// minimizing the number of declared requirements.
//
// The returned affinity is verified cluster-by-cluster through the same
// util.ClusterMatches predicate invoked by the ClusterAffinity scheduler
// filter: every target cluster matches and no non-target cluster matches.
// Guarantees are scoped to the snapshot identity embedded in the result;
// clusters outside the snapshot are not modeled.
func SuggestClusterAffinity(req SuggestClusterAffinityRequest) (*SuggestClusterAffinityResult, error) {
	norm, err := normalizeRequest(req)
	if err != nil {
		return nil, err
	}

	identity := buildSnapshotIdentity(norm.uids)
	targetUIDs := sets.List(norm.targets)

	signatures := make([]map[string]sigToken, len(norm.clusters))
	for i, cluster := range norm.clusters {
		signatures[i] = signatureOf(cluster.GetLabels(), norm.vocab)
	}

	witnesses := findWitnesses(norm, signatures)
	if len(witnesses) > 0 {
		return infeasibleResult(identity, targetUIDs, witnesses,
			"target and non-target clusters share an identical label signature within the allowed vocabulary"), nil
	}

	targetPositions, nonTargetPositions := partitionPositions(norm)
	if len(nonTargetPositions) == 0 {
		// Every snapshot cluster is a target: the empty label selector matches
		// the whole snapshot at zero cost.
		affinity := buildAffinity(nil, nil)
		return verifiedResult(norm, identity, targetUIDs, affinity, StatusOptimal, 0, 0, 0,
			nil, false, "empty label selector selects every cluster of the snapshot")
	}

	cands, err := generateCandidates(norm, targetPositions, nonTargetPositions)
	if err != nil {
		return nil, err
	}
	if len(cands) == 0 {
		return infeasibleResult(identity, targetUIDs, nil,
			"no vocabulary-restricted label requirement can preserve every target cluster, "+
				"so no conjunction within the fixed grammar can separate the target set"), nil
	}
	if len(cands) > maxCandidateRequirements {
		return &SuggestClusterAffinityResult{
			Status:           StatusIncomplete,
			Snapshot:         identity,
			TargetUIDs:       targetUIDs,
			ProvenLowerBound: 0,
			BudgetExhausted:  false,
			Message: fmt.Sprintf("candidate family of %d requirements exceeds the fixed cap of %d; "+
				"reduce the vocabulary or snapshot size", len(cands), maxCandidateRequirements),
		}, nil
	}

	sv := newSolver(cands, sets.New(nonTargetPositions...), norm.maxReq, norm.searchBudget)
	sv.run()

	if sv.best == nil {
		if sv.exhausted {
			return &SuggestClusterAffinityResult{
				Status:           StatusIncomplete,
				Snapshot:         identity,
				TargetUIDs:       targetUIDs,
				ProvenLowerBound: sv.rootLowerBound,
				NodesEvaluated:   sv.nodes,
				BudgetExhausted:  true,
				Message:          "search node budget exhausted before any exact expression was found",
			}, nil
		}
		return infeasibleResult(identity, targetUIDs, nil,
			fmt.Sprintf("separating the target set requires more than the fixed maximum of %d requirements",
				norm.maxReq)), nil
	}

	affinity := buildAffinity(sv.best, cands)
	if sv.exhausted {
		return verifiedResult(norm, identity, targetUIDs, affinity, StatusIncomplete,
			sv.rootLowerBound, len(sv.best), sv.nodes, nil, true,
			"search node budget exhausted; the expression is exact but only bounded, not proven minimal")
	}

	return verifiedResult(norm, identity, targetUIDs, affinity, StatusOptimal,
		len(sv.best), len(sv.best), sv.nodes, nil, false,
		"branch-and-bound search completed and proved the minimum requirement count")
}

// MatchedUIDs evaluates an affinity against a snapshot with the real
// util.ClusterMatches predicate and returns the matching cluster UIDs in
// stable sorted order.
func MatchedUIDs(snapshot []*clusterv1alpha1.Cluster, affinity policyv1alpha1.ClusterAffinity) []types.UID {
	matched := sets.New[types.UID]()
	for _, cluster := range snapshot {
		if util.ClusterMatches(cluster, affinity) {
			matched.Insert(cluster.UID)
		}
	}
	return sets.List(matched)
}

// normalizeRequest validates the request and produces deterministic,
// deduplicated inputs.
func normalizeRequest(req SuggestClusterAffinityRequest) (*normalizedRequest, error) {
	uids, err := validateSnapshot(req.Snapshot)
	if err != nil {
		return nil, err
	}
	targets, err := validateTargetUIDs(req.TargetUIDs, sets.New(uids...))
	if err != nil {
		return nil, err
	}
	vocab, err := validateVocabulary(req.Vocabulary)
	if err != nil {
		return nil, err
	}
	maxReq, err := normalizeMaxRequirements(req.MaxRequirements)
	if err != nil {
		return nil, err
	}
	return &normalizedRequest{
		clusters:     req.Snapshot,
		uids:         uids,
		targets:      targets,
		vocab:        vocab,
		maxReq:       maxReq,
		searchBudget: normalizeBudget(req.SearchNodeBudget),
	}, nil
}

// validateSnapshot ensures the snapshot is non-empty, its entries are non-nil
// and its UIDs are non-empty and unique, and returns the UIDs sorted.
func validateSnapshot(snapshot []*clusterv1alpha1.Cluster) ([]types.UID, error) {
	if len(snapshot) == 0 {
		return nil, fmt.Errorf("%w: snapshot must contain at least one cluster", ErrInvalidRequest)
	}

	seenUIDs := sets.New[types.UID]()
	uids := make([]types.UID, 0, len(snapshot))
	for i, cluster := range snapshot {
		if cluster == nil {
			return nil, fmt.Errorf("%w: snapshot entry %d is nil", ErrInvalidRequest, i)
		}
		if cluster.UID == "" {
			return nil, fmt.Errorf("%w: cluster at snapshot index %d has an empty metadata.uid", ErrInvalidRequest, i)
		}
		if seenUIDs.Has(cluster.UID) {
			return nil, fmt.Errorf("%w: duplicate cluster uid %q in snapshot", ErrInvalidRequest, cluster.UID)
		}
		seenUIDs.Insert(cluster.UID)
		uids = append(uids, cluster.UID)
	}
	slices.Sort(uids)
	return uids, nil
}

// validateTargetUIDs ensures the target set is non-empty, free of empty
// entries and fully contained in the snapshot.
func validateTargetUIDs(targetUIDs []types.UID, knownUIDs sets.Set[types.UID]) (sets.Set[types.UID], error) {
	if len(targetUIDs) == 0 {
		return nil, fmt.Errorf("%w: target UID set must be non-empty", ErrInvalidRequest)
	}

	targets := sets.New[types.UID]()
	for _, uid := range targetUIDs {
		if uid == "" {
			return nil, fmt.Errorf("%w: target UIDs must not be empty", ErrInvalidRequest)
		}
		if !knownUIDs.Has(uid) {
			return nil, fmt.Errorf("%w: target uid %q is not part of the authorized snapshot", ErrInvalidRequest, uid)
		}
		targets.Insert(uid)
	}
	return targets, nil
}

// validateVocabulary ensures the vocabulary is non-empty and every key/value
// passes the authoritative label syntax checks.
func validateVocabulary(vocabulary Vocabulary) (map[string]sets.Set[string], error) {
	if len(vocabulary.AllowedValues) == 0 {
		return nil, fmt.Errorf("%w: vocabulary must declare at least one allowed label key", ErrInvalidRequest)
	}

	vocab := make(map[string]sets.Set[string], len(vocabulary.AllowedValues))
	for key, values := range vocabulary.AllowedValues {
		if key == "" {
			return nil, fmt.Errorf("%w: vocabulary contains an empty label key", ErrInvalidRequest)
		}
		// Probe the real label selector constructor so invalid key/value
		// syntax is rejected up front with the authoritative validator.
		if _, err := labels.NewRequirement(key, selection.Exists, nil); err != nil {
			return nil, fmt.Errorf("%w: invalid label key %q: %v", ErrInvalidRequest, key, err)
		}
		valueSet := sets.New[string]()
		for _, value := range values {
			if _, err := labels.NewRequirement(key, selection.In, []string{value}); err != nil {
				return nil, fmt.Errorf("%w: invalid label value %q for key %q: %v",
					ErrInvalidRequest, value, key, err)
			}
			valueSet.Insert(value)
		}
		vocab[key] = valueSet
	}
	return vocab, nil
}

// normalizeMaxRequirements applies the fixed grammar cap.
func normalizeMaxRequirements(maxReq int) (int, error) {
	if maxReq <= 0 {
		return MaxRequirements, nil
	}
	if maxReq > MaxRequirements {
		return 0, fmt.Errorf("%w: MaxRequirements %d exceeds the fixed grammar limit of %d",
			ErrInvalidRequest, maxReq, MaxRequirements)
	}
	return maxReq, nil
}

// normalizeBudget applies the default branch-and-bound node budget.
func normalizeBudget(budget int64) int64 {
	if budget <= 0 {
		return DefaultSearchNodeBudget
	}
	return budget
}

// buildSnapshotIdentity derives a stable identity from the sorted snapshot
// UIDs. Ordering differences in the input snapshot do not change the identity.
func buildSnapshotIdentity(uids []types.UID) SnapshotIdentity {
	sorted := slices.Clone(uids)
	slices.Sort(sorted)

	var builder strings.Builder
	for _, uid := range sorted {
		builder.WriteString(strconv.Itoa(len(uid)))
		builder.WriteString(":")
		builder.WriteString(string(uid))
		builder.WriteString("\n")
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return SnapshotIdentity{
		ID:           hex.EncodeToString(sum[:])[:snapshotIDHexLength],
		ClusterCount: len(sorted),
		UIDs:         sorted,
	}
}

// signatureOf projects the given labels onto the allowed vocabulary. The
// projection distinguishes missing keys from present values and allowed
// values from unmentionable ones.
func signatureOf(clusterLabels map[string]string, vocab map[string]sets.Set[string]) map[string]sigToken {
	signature := make(map[string]sigToken, len(vocab))
	for key, allowedValues := range vocab {
		value, present := clusterLabels[key]
		switch {
		case !present:
			signature[key] = sigToken{present: false}
		case allowedValues.Has(value):
			signature[key] = sigToken{present: true, allowed: true, value: value}
		default:
			signature[key] = sigToken{present: true, allowed: false}
		}
	}
	return signature
}

// canonicalSignature renders a signature as a collision-free stable string.
func canonicalSignature(signature map[string]sigToken) string {
	keys := make([]string, 0, len(signature))
	for key := range signature {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	entries := make([]string, 0, len(keys))
	for _, key := range keys {
		token := signature[key]
		switch {
		case !token.present:
			entries = append(entries, key+canonicalFieldSep+"absent")
		case !token.allowed:
			entries = append(entries, key+canonicalFieldSep+unmentionableToken)
		default:
			entries = append(entries, key+canonicalFieldSep+token.value)
		}
	}
	return strings.Join(entries, canonicalEntrySep)
}

// findWitnesses returns every target/non-target pair whose vocabulary
// projection is identical. The pairs are stably sorted by both UIDs.
func findWitnesses(norm *normalizedRequest, signatures []map[string]sigToken) []IndistinguishablePair {
	type signatureGroup struct {
		targets    []types.UID
		nonTargets []types.UID
	}
	groups := make(map[string]signatureGroup)
	for i, cluster := range norm.clusters {
		group := groups[canonicalSignature(signatures[i])]
		if norm.targets.Has(cluster.UID) {
			group.targets = append(group.targets, cluster.UID)
		} else {
			group.nonTargets = append(group.nonTargets, cluster.UID)
		}
		groups[canonicalSignature(signatures[i])] = group
	}

	var witnesses []IndistinguishablePair
	for _, group := range groups {
		if len(group.targets) == 0 || len(group.nonTargets) == 0 {
			continue
		}
		for _, targetUID := range group.targets {
			for _, nonTargetUID := range group.nonTargets {
				witnesses = append(witnesses, IndistinguishablePair{
					TargetUID:    targetUID,
					NonTargetUID: nonTargetUID,
				})
			}
		}
	}
	slices.SortFunc(witnesses, func(a, b IndistinguishablePair) int {
		if a.TargetUID != b.TargetUID {
			return cmpStrings(string(a.TargetUID), string(b.TargetUID))
		}
		return cmpStrings(string(a.NonTargetUID), string(b.NonTargetUID))
	})
	return witnesses
}

// partitionPositions returns the UID-sorted positions of target and
// non-target clusters.
func partitionPositions(norm *normalizedRequest) (targetPositions, nonTargetPositions []int) {
	order := make([]int, len(norm.clusters))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(i, j int) int {
		return cmpStrings(string(norm.clusters[i].UID), string(norm.clusters[j].UID))
	})
	for _, pos := range order {
		if norm.targets.Has(norm.clusters[pos].UID) {
			targetPositions = append(targetPositions, pos)
		} else {
			nonTargetPositions = append(nonTargetPositions, pos)
		}
	}
	return targetPositions, nonTargetPositions
}

// generateCandidates enumerates the finite requirement family implied by the
// vocabulary and snapshot. A candidate is retained only when it matches every
// target cluster and rejects at least one non-target cluster.
func generateCandidates(norm *normalizedRequest,
	targetPositions, nonTargetPositions []int,
) ([]candidate, error) {
	keys := make([]string, 0, len(norm.vocab))
	for key := range norm.vocab {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	var cands []candidate
	seen := sets.New[string]()
	for _, key := range keys {
		presentValues := collectPresentValues(norm, key)
		for _, expr := range rawRequirements(key, presentValues) {
			canonical := canonicalRequirement(expr)
			if seen.Has(canonical) {
				continue
			}
			selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{expr},
			})
			if err != nil {
				return nil, fmt.Errorf("internal error: synthesized requirement is rejected by the label selector: %w", err)
			}

			matchesAllTargets := true
			for _, pos := range targetPositions {
				if !selector.Matches(labels.Set(norm.clusters[pos].GetLabels())) {
					matchesAllTargets = false
					break
				}
			}
			if !matchesAllTargets {
				continue
			}

			kill := sets.New[int]()
			for _, pos := range nonTargetPositions {
				if !selector.Matches(labels.Set(norm.clusters[pos].GetLabels())) {
					kill.Insert(pos)
				}
			}
			if kill.Len() == 0 {
				continue
			}

			seen.Insert(canonical)
			cands = append(cands, candidate{
				expr:      expr,
				canonical: canonical,
				kill:      kill,
			})
		}
	}

	slices.SortFunc(cands, func(a, b candidate) int {
		return cmpStrings(a.canonical, b.canonical)
	})
	return cands, nil
}

// collectPresentValues returns the sorted distinct values of key that are
// present in the snapshot and allowed by the vocabulary.
func collectPresentValues(norm *normalizedRequest, key string) []string {
	allowed := norm.vocab[key]
	values := sets.New[string]()
	for _, cluster := range norm.clusters {
		value, present := cluster.GetLabels()[key]
		if present && allowed.Has(value) {
			values.Insert(value)
		}
	}
	return sets.List(values)
}

// rawRequirements enumerates every finite requirement form for one key using
// the real label selector operators: Exists, DoesNotExist, and In/NotIn over
// every non-empty subset of the present allowed values.
func rawRequirements(key string, presentValues []string) []metav1.LabelSelectorRequirement {
	reqs := []metav1.LabelSelectorRequirement{
		{Key: key, Operator: metav1.LabelSelectorOpExists},
		{Key: key, Operator: metav1.LabelSelectorOpDoesNotExist},
	}

	// Masks enumerate subsets in numeric order, which is deterministic and
	// yields values in sorted order within each subset.
	for mask := 1; mask < (1 << len(presentValues)); mask++ {
		subset := make([]string, 0, len(presentValues))
		for bit, value := range presentValues {
			if mask&(1<<bit) != 0 {
				subset = append(subset, value)
			}
		}
		reqs = append(reqs,
			metav1.LabelSelectorRequirement{Key: key, Operator: metav1.LabelSelectorOpIn, Values: subset},
			metav1.LabelSelectorRequirement{Key: key, Operator: metav1.LabelSelectorOpNotIn, Values: slices.Clone(subset)},
		)
	}
	return reqs
}

// canonicalRequirement renders a requirement as a stable, collision-free
// string used for sorting and de-duplication.
func canonicalRequirement(expr metav1.LabelSelectorRequirement) string {
	return expr.Key + canonicalFieldSep + string(expr.Operator) +
		canonicalFieldSep + strings.Join(expr.Values, canonicalFieldSep)
}

// solver runs the minimum set-cover branch-and-bound search over the finite
// candidate family. Each candidate "kills" a set of non-target clusters; the
// goal is to kill every non-target with as few requirements as possible.
type solver struct {
	cands    []candidate
	universe sets.Set[int]
	maxReq   int
	budget   int64

	nodes          int64
	exhausted      bool
	maxKill        int
	best           []int
	rootLowerBound int
}

// newSolver constructs a solver with a greedy upper bound and an admissible
// lower bound already computed.
func newSolver(cands []candidate, universe sets.Set[int], maxReq int, budget int64) *solver {
	sv := &solver{
		cands:    cands,
		universe: universe,
		maxReq:   maxReq,
		budget:   budget,
	}
	for _, cand := range cands {
		if cand.kill.Len() > sv.maxKill {
			sv.maxKill = cand.kill.Len()
		}
	}
	sv.best = sv.greedyCover()
	sv.rootLowerBound = sv.packingLowerBound(universe)
	return sv
}

// run executes the depth-first branch-and-bound search.
func (sv *solver) run() {
	if sv.maxKill == 0 {
		return
	}
	sv.dfs(0, sv.universe, nil)
}

// dfs explores requirement combinations in canonical index order. Candidate
// indices only increase, so each combination is visited once in a fixed order.
func (sv *solver) dfs(start int, uncovered sets.Set[int], chosen []int) {
	sv.nodes++
	if sv.nodes > sv.budget {
		sv.exhausted = true
		return
	}
	if uncovered.Len() == 0 {
		sv.accept(chosen)
		return
	}
	if len(chosen) >= sv.maxReq {
		return
	}
	if sv.best != nil && len(chosen) >= len(sv.best) {
		return
	}
	// Admissible prune: even a requirement with maximum reach cannot beat the
	// incumbent once this lower bound strictly exceeds its length.
	quickBound := len(chosen) + ceilDiv(uncovered.Len(), sv.maxKill)
	if sv.best != nil && quickBound > len(sv.best) {
		return
	}

	for i := start; i < len(sv.cands); i++ {
		added := sv.cands[i].kill.Intersection(uncovered)
		if added.Len() == 0 {
			continue
		}
		sv.dfs(i+1, uncovered.Difference(sv.cands[i].kill), append(chosen, i))
		if sv.exhausted {
			return
		}
	}
}

// accept records a complete cover when it is shorter than the incumbent or, at
// equal length, canonically smaller.
func (sv *solver) accept(chosen []int) {
	candidateIDs := sortedCandidateIDs(chosen)
	if sv.best == nil || better(candidateIDs, sv.best, sv.cands) {
		sv.best = candidateIDs
	}
}

// greedyCover builds a deterministic feasible cover as the initial upper bound.
func (sv *solver) greedyCover() []int {
	uncovered := sv.universe.Clone()
	chosen := make([]int, 0, sv.maxReq)
	for uncovered.Len() > 0 {
		bestIdx, bestAdd := -1, 0
		for i, cand := range sv.cands {
			added := cand.kill.Intersection(uncovered).Len()
			if added > bestAdd {
				bestIdx, bestAdd = i, added
			}
		}
		if bestIdx < 0 {
			return nil
		}
		chosen = append(chosen, bestIdx)
		if len(chosen) > sv.maxReq {
			return nil
		}
		uncovered = uncovered.Difference(sv.cands[bestIdx].kill)
	}
	return sortedCandidateIDs(chosen)
}

// packingLowerBound returns an admissible lower bound by repeatedly removing
// as many remaining non-targets as the largest candidate reach could cover.
func (sv *solver) packingLowerBound(initial sets.Set[int]) int {
	if sv.maxKill == 0 {
		return sv.maxReq + 1
	}
	remaining := initial.Clone()
	bound := 0
	for remaining.Len() > 0 {
		bound++
		if bound > sv.maxReq {
			return sv.maxReq + 1
		}
		positions := sets.List(remaining)
		if len(positions) <= sv.maxKill {
			break
		}
		remaining = sets.New(positions[sv.maxKill:]...)
	}
	return bound
}

// buildAffinity turns chosen candidate indices into a ClusterAffinity whose
// requirements are canonically sorted. A nil choice yields an empty label
// selector that matches everything.
func buildAffinity(chosen []int, cands []candidate) *policyv1alpha1.ClusterAffinity {
	exprs := make([]metav1.LabelSelectorRequirement, 0, len(chosen))
	for _, idx := range chosen {
		exprs = append(exprs, cands[idx].expr)
	}
	slices.SortFunc(exprs, func(a, b metav1.LabelSelectorRequirement) int {
		return cmpStrings(canonicalRequirement(a), canonicalRequirement(b))
	})
	return &policyv1alpha1.ClusterAffinity{
		LabelSelector: &metav1.LabelSelector{
			MatchExpressions: exprs,
		},
	}
}

// verifiedResult assembles a result carrying an affinity and verifies it
// against every snapshot cluster through the real ClusterMatches predicate.
func verifiedResult(norm *normalizedRequest, identity SnapshotIdentity, targetUIDs []types.UID,
	affinity *policyv1alpha1.ClusterAffinity, status Status, lowerBound, upperBound int,
	nodes int64, witnesses []IndistinguishablePair, budgetExhausted bool, message string,
) (*SuggestClusterAffinityResult, error) {
	matched := MatchedUIDs(norm.clusters, *affinity)
	if !sets.New(targetUIDs...).Equal(sets.New(matched...)) {
		// Unreachable: the candidate engine and ClusterMatches use the same
		// label selector semantics. Return an error instead of handing back an
		// unverified suggestion.
		return nil, fmt.Errorf("internal error: synthesized affinity verification mismatch; targets=%v matched=%v",
			targetUIDs, matched)
	}
	return &SuggestClusterAffinityResult{
		Status:           status,
		Affinity:         affinity,
		Snapshot:         identity,
		TargetUIDs:       slices.Clone(targetUIDs),
		MatchedUIDs:      matched,
		RequirementCount: len(affinity.LabelSelector.MatchExpressions),
		ProvenLowerBound: lowerBound,
		ProvenUpperBound: upperBound,
		Witnesses:        witnesses,
		NodesEvaluated:   nodes,
		BudgetExhausted:  budgetExhausted,
		Message:          message,
	}, nil
}

// infeasibleResult assembles an Infeasible result without an affinity.
func infeasibleResult(identity SnapshotIdentity, targetUIDs []types.UID,
	witnesses []IndistinguishablePair, message string,
) *SuggestClusterAffinityResult {
	return &SuggestClusterAffinityResult{
		Status:     StatusInfeasible,
		Snapshot:   identity,
		TargetUIDs: slices.Clone(targetUIDs),
		Witnesses:  witnesses,
		Message:    message,
	}
}

// sortedCandidateIDs returns candidate indices in increasing order, which is
// also canonical order because the candidate slice is canonically sorted.
func sortedCandidateIDs(indices []int) []int {
	sorted := slices.Clone(indices)
	slices.Sort(sorted)
	return sorted
}

// better reports whether candidate combination a is preferable to incumbent b
// under the (minimum size, canonical lexicographic order) objective.
func better(a, b []int, cands []candidate) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	for i := range a {
		left, right := cands[a[i]].canonical, cands[b[i]].canonical
		if left != right {
			return left < right
		}
	}
	return false
}

// ceilDiv returns the ceiling of a / b for positive b.
func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}

// cmpStrings compares two strings deterministically.
func cmpStrings(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
