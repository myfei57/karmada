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
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strconv"

	"github.com/tidwall/gjson"
	"k8s.io/apimachinery/pkg/util/sets"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
)

// protectedIdentityFields are JSON paths that identify a resource. A candidate
// script is never allowed to rewrite them, even when the rule lists a path in
// AllowedDiffFields: identity changes would corrupt Work/replica decisions and
// are reported as IdentityViolation.
var protectedIdentityFields = []string{
	"apiVersion",
	"kind",
	"metadata.name",
	"metadata.namespace",
	"metadata.uid",
}

// maxErrorMessageLength bounds retained candidate error text. Candidate scripts
// may raise arbitrary messages, so only a truncated prefix is ever retained.
const maxErrorMessageLength = 256

// replicaComparison holds the scalar GetReplicas outcome.
type replicaComparison struct {
	outcome    Outcome
	diffFields []string
}

// compareReplicas classifies a candidate GetReplicas result against the active one.
func compareReplicas(activeReplica int32, activeRequires, candidateRequires *workv1alpha2.ReplicaRequirements, candidateReplica int32) replicaComparison {
	diffFields := make([]string, 0, 2)
	if activeReplica != candidateReplica {
		diffFields = append(diffFields, "replicas")
	}
	if !reflect.DeepEqual(activeRequires, candidateRequires) {
		diffFields = append(diffFields, "replicaRequirements")
	}
	if len(diffFields) == 0 {
		return replicaComparison{outcome: OutcomeMatch}
	}
	return replicaComparison{outcome: OutcomeDiff, diffFields: diffFields}
}

// compareHealth classifies a candidate InterpretHealth boolean against the active one.
func compareHealth(activeHealthy, candidateHealthy bool) (Outcome, []string) {
	if activeHealthy == candidateHealthy {
		return OutcomeMatch, nil
	}
	return OutcomeDiff, []string{"healthy"}
}

// reviseComparison holds the ReviseReplica outcome with structural diff paths.
type reviseComparison struct {
	outcome    Outcome
	diffFields []string
}

// compareRevisedObject classifies a candidate revised object against the active
// revised object. Identity fields are always protected. Remaining differences are
// tolerated only when every differing path is explicitly allowed by the rule.
func compareRevisedObject(activeJSON, candidateJSON []byte, allowedFields sets.Set[string]) reviseComparison {
	active := gjson.ParseBytes(activeJSON)
	candidate := gjson.ParseBytes(candidateJSON)

	identityViolations := collectIdentityViolations(active, candidate)
	diffFields := collectDiffPaths(active, candidate, "")

	if len(identityViolations) > 0 {
		// Report identity paths first, then any remaining structural differences,
		// keeping the whole list bounded.
		merged := append([]string{}, identityViolations...)
		for _, path := range diffFields {
			if !sets.New(identityViolations...).Has(path) {
				merged = append(merged, path)
			}
		}
		return reviseComparison{outcome: OutcomeIdentityViolation, diffFields: capFields(merged)}
	}

	if len(diffFields) == 0 {
		return reviseComparison{outcome: OutcomeMatch}
	}

	if allowedFields != nil {
		allAllowed := true
		for _, path := range diffFields {
			if !allowedFields.Has(path) {
				allAllowed = false
				break
			}
		}
		if allAllowed {
			return reviseComparison{outcome: OutcomeAllowedDiff, diffFields: capFields(diffFields)}
		}
	}
	return reviseComparison{outcome: OutcomeDiff, diffFields: capFields(diffFields)}
}

// collectIdentityViolations returns the protected identity paths whose presence or
// value differs between the two documents.
func collectIdentityViolations(active, candidate gjson.Result) []string {
	violations := make([]string, 0, len(protectedIdentityFields))
	for _, path := range protectedIdentityFields {
		activeValue := active.Get(path)
		candidateValue := candidate.Get(path)
		if activeValue.Raw != candidateValue.Raw || activeValue.Exists() != candidateValue.Exists() {
			violations = append(violations, path)
		}
	}
	return violations
}

// collectDiffPaths walks two JSON documents and returns the dotted paths whose
// values differ. Only structural paths are retained, never values, so the result
// is safe to log without exposing secret state.
func collectDiffPaths(active, candidate gjson.Result, path string) []string {
	diffFields := make([]string, 0)
	collectDiffPathsRecursive(active, candidate, path, &diffFields)
	return capFields(diffFields)
}

func collectDiffPathsRecursive(active, candidate gjson.Result, path string, diffFields *[]string) {
	if len(*diffFields) >= maxReportedDiffFields {
		return
	}

	switch {
	case active.IsObject() && candidate.IsObject():
		keys := sets.New[string]()
		activeMap := active.Map()
		candidateMap := candidate.Map()
		for key := range activeMap {
			keys.Insert(key)
		}
		for key := range candidateMap {
			keys.Insert(key)
		}
		for _, key := range keys.UnsortedList() {
			childPath := joinJSONPath(path, key)
			activeChild, activeOK := activeMap[key]
			candidateChild, candidateOK := candidateMap[key]
			if activeOK != candidateOK {
				*diffFields = append(*diffFields, childPath)
				continue
			}
			collectDiffPathsRecursive(activeChild, candidateChild, childPath, diffFields)
			if len(*diffFields) >= maxReportedDiffFields {
				return
			}
		}
	case active.IsArray() && candidate.IsArray():
		activeArray := active.Array()
		candidateArray := candidate.Array()
		if len(activeArray) != len(candidateArray) {
			*diffFields = append(*diffFields, path)
			return
		}
		for index := range activeArray {
			collectDiffPathsRecursive(activeArray[index], candidateArray[index], joinJSONPath(path, gjsonIndex(index)), diffFields)
			if len(*diffFields) >= maxReportedDiffFields {
				return
			}
		}
	default:
		if active.Raw != candidate.Raw {
			*diffFields = append(*diffFields, path)
		}
	}
}

// joinJSONPath joins a parent path and a key using gjson/sjson dotted conventions.
func joinJSONPath(parent, key string) string {
	if parent == "" {
		return key
	}
	return parent + "." + key
}

// gjsonIndex renders an array index as a dotted path segment.
func gjsonIndex(index int) string {
	return strconv.Itoa(index)
}

// capFields caps a field path list at maxReportedDiffFields, preserving order.
func capFields(fields []string) []string {
	if len(fields) <= maxReportedDiffFields {
		return fields
	}
	return fields[:maxReportedDiffFields]
}

// digestBytes returns a short sha256 hex digest of raw bytes.
func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])[:digestLength]
}
