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

package overridemanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/go-openapi/jsonpointer"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"github.com/karmada-io/karmada/pkg/events"
	"github.com/karmada-io/karmada/pkg/features"
)

const (
	// OverrideOriginGraphSchemaVersion is the payload version of OverrideOriginGraph.
	OverrideOriginGraphSchemaVersion = "v1alpha1"

	// Provenance levels describe where the value of a traced field comes from.
	// SourceLevelPolicy means the write is precisely attributed to a concrete policy overrider.
	SourceLevelPolicy = "Policy"
	// SourceLevelInput means the value was inherited from the original resource input and no
	// policy ever wrote it.
	SourceLevelInput = "Input"
	// SourceLevelInputLevel means the fragment lives inside an embedded document (e.g. a YAML
	// snippet stored in a ConfigMap key) and no precise source line can be known. Line numbers
	// must never be fabricated for such fragments.
	SourceLevelInputLevel = "InputLevel"

	// Lifecycle states of a field write record.
	writeStateActive     = "active"
	writeStateSuperseded = "superseded"
	writeStateDeleted    = "deleted"

	// Overrider kind labels carried by every traced write.
	originKindImage       = "ImageOverrider"
	originKindCommand     = "CommandOverrider"
	originKindArgs        = "ArgsOverrider"
	originKindLabels      = "LabelsOverrider"
	originKindAnnotations = "AnnotationsOverrider"
	originKindFieldJSON   = "FieldOverriderJSON"
	originKindFieldYAML   = "FieldOverriderYAML"
	originKindPlaintext   = "PlaintextOverrider"

	// Truncation reasons explain which limit a graph hit.
	truncationReasonFields     = "FieldsLimitReached"
	truncationReasonOperations = "OperationsLimitReached"
	truncationReasonOutput     = "OutputBytesLimitReached"

	// Tracing budgets. They only bound the graph payload; patch application itself is never skipped.
	defaultMaxGraphFields      = 4096
	defaultMaxGraphOperations  = 4096
	defaultMaxGraphOutputBytes = 1 << 20
	// maxValuePreviewBytes caps a single non-sensitive value embedded in the graph. Full-text
	// policy configuration is never retained.
	maxValuePreviewBytes = 256
	// embeddedPathSeparator joins the outer FieldPath and the inner document SubPath, e.g.
	// "/data/test.yaml#/key/key1".
	embeddedPathSeparator = "#"
)

// PolicyRef identifies the exact policy revision that produced a write.
type PolicyRef struct {
	// Kind is either OverridePolicy or ClusterOverridePolicy.
	Kind string `json:"kind"`
	// Namespace is empty for ClusterOverridePolicy.
	Namespace string `json:"namespace,omitempty"`
	// Name is the policy name.
	Name string `json:"name"`
	// UID is the policy object UID.
	UID string `json:"uid,omitempty"`
	// Generation pins the policy revision.
	Generation int64 `json:"generation,omitempty"`
}

// ResourceRef identifies the traced resource.
type ResourceRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace,omitempty"`
	Name       string `json:"name"`
	UID        string `json:"uid,omitempty"`
}

// ClusterRef identifies the target member cluster.
type ClusterRef struct {
	Name string `json:"name"`
	UID  string `json:"uid,omitempty"`
}

// ContainerIdentity pins a write to the actual container position and name instead of
// attributing it to the whole resource.
type ContainerIdentity struct {
	// Type is "containers" or "initContainers".
	Type string `json:"type"`
	// Index is the real list index at the time the write happened.
	Index int `json:"index"`
	// Name is the container name resolved from the live object.
	Name string `json:"name,omitempty"`
}

// ArrayShift records a real list index movement caused by an add/remove operation.
type ArrayShift struct {
	// ArrayPath is the JSON pointer of the affected list.
	ArrayPath string `json:"arrayPath"`
	// FieldPath is the current (post-shift) path of the moved tracked field.
	FieldPath string `json:"fieldPath"`
	// FromIndex and ToIndex describe the observed movement.
	FromIndex int `json:"fromIndex"`
	ToIndex   int `json:"toIndex"`
}

// FieldWriteRecord is one actual write performed on a field by one overrider.
type FieldWriteRecord struct {
	// Sequence is the global order of the write within the run.
	Sequence int `json:"seq"`
	// Policy is the exact policy revision that performed the write.
	Policy PolicyRef `json:"policy"`
	// OverriderKind is the kind of overrider that produced the write.
	OverriderKind string `json:"overriderKind"`
	// OverriderIndex is the position of the overrider inside its policy Overriders list.
	OverriderIndex int `json:"overriderIndex"`
	// Op is the JSON patch operation: add, replace or remove.
	Op string `json:"op"`
	// SourceLevel is always SourceLevelPolicy for a write record.
	SourceLevel string `json:"sourceLevel"`
	// State is active, superseded (overwritten by a later write) or deleted (removed later).
	State string `json:"state"`
	// SupersededBy points at the sequence number of the write that overwrote or deleted it.
	SupersededBy *int `json:"supersededBy,omitempty"`
	// Container pins container-scoped writes (image, command, args).
	Container *ContainerIdentity `json:"container,omitempty"`
	// ImageComponent/ImageOperator capture the image split (Registry/Repository/Tag) that was
	// rewritten even though the whole image string is replaced.
	ImageComponent string `json:"imageComponent,omitempty"`
	ImageOperator  string `json:"imageOperator,omitempty"`
	// Value holds the written value for non-sensitive fields. It is nil for sensitive fields.
	Value any `json:"value,omitempty"`
	// ValueSummary is the only value evidence retained for sensitive fields (type/length/hash).
	ValueSummary string `json:"valueSummary,omitempty"`
	// Sensitive marks redacted writes.
	Sensitive bool `json:"sensitive,omitempty"`
}

// FieldOrigin is the full lineage of one concrete field path.
type FieldOrigin struct {
	// Path is the current RFC6901 JSON pointer of the field. For embedded document fragments it
	// is composed as "<FieldPath>#<SubPath>". Array movements are reflected by updating this path.
	Path string `json:"path"`
	// EmbeddedFieldPath is set when the field lives inside a FieldOverrider JSON/YAML document.
	EmbeddedFieldPath string `json:"embeddedFieldPath,omitempty"`
	// EmbeddedSubPath is the pointer inside the embedded document.
	EmbeddedSubPath string `json:"embeddedSubPath,omitempty"`
	// ExistedInInput reports whether the field or fragment existed on the pristine input.
	ExistedInInput bool `json:"existedInInput,omitempty"`
	// InputSourceLevel is the provenance level of the value inherited from the input.
	InputSourceLevel string `json:"inputSourceLevel,omitempty"`
	// Removed marks nodes whose list element was deleted; their Path is the last occupied path
	// and FinalOwner is always nil. They retain deletion evidence instead of disappearing.
	Removed bool `json:"removed,omitempty"`
	// Writes is the ordered write evidence for this field.
	Writes []FieldWriteRecord `json:"writes"`
	// FinalOwner is the sequence of the write that produced the final value. It is nil when the
	// field has been deleted. It is resolved from real write records, never guessed from names.
	FinalOwner *int `json:"finalOwner,omitempty"`
}

// OperationRecord is one applied patch operation with its observable effects. It deliberately
// carries no full-text value, only a summary, so the graph cannot become a full configuration dump.
type OperationRecord struct {
	Sequence          int                `json:"seq"`
	Policy            PolicyRef          `json:"policy"`
	OverriderKind     string             `json:"overriderKind"`
	OverriderIndex    int                `json:"overriderIndex"`
	Op                string             `json:"op"`
	Path              string             `json:"path"`
	EmbeddedFieldPath string             `json:"embeddedFieldPath,omitempty"`
	EmbeddedSubPath   string             `json:"embeddedSubPath,omitempty"`
	Container         *ContainerIdentity `json:"container,omitempty"`
	Sensitive         bool               `json:"sensitive,omitempty"`
	ValueSummary      string             `json:"valueSummary,omitempty"`
	ShiftedFields     []ArrayShift       `json:"shiftedFields,omitempty"`
}

// GraphLimits caps graph size so tracing cannot exhaust memory or output.
type GraphLimits struct {
	MaxFields      int `json:"maxFields"`
	MaxOperations  int `json:"maxOperations"`
	MaxOutputBytes int `json:"maxOutputBytes"`
}

// OverrideOriginGraph is the field-level provenance graph of one override run for a fixed
// resource and cluster input. It is bound to the exact policy revisions, cluster identity and
// pristine template digest involved in the run.
type OverrideOriginGraph struct {
	SchemaVersion string      `json:"schemaVersion"`
	Resource      ResourceRef `json:"resource"`
	Cluster       ClusterRef  `json:"cluster"`
	// TemplateDigest is the sha256 of the pristine resource JSON before any override.
	TemplateDigest string            `json:"templateDigest"`
	Policies       []PolicyRef       `json:"policies"`
	Fields         []FieldOrigin     `json:"fields"`
	Operations     []OperationRecord `json:"operations,omitempty"`
	// Truncated is true when one of the configured budgets was hit; the graph then honestly
	// reports incompleteness instead of pretending to be whole.
	Truncated         bool        `json:"truncated,omitempty"`
	TruncationReasons []string    `json:"truncationReasons,omitempty"`
	Limits            GraphLimits `json:"limits"`
}

// traceMeta carries the provenance of the overrider currently being applied.
type traceMeta struct {
	policy         PolicyRef
	kind           string
	index          int
	imageComponent string
	imageOperator  string
	embeddedField  string
	embeddedSub    string
	inputLevel     string
}

// originTracer records evidence while the real, unmodified patch builders and appliers run.
// It never re-implements patch semantics; it only observes the same overrideOption patches that
// ApplyOverridePolicies would apply, so the traced final object stays identical.
type originTracer struct {
	graph    *OverrideOriginGraph
	target   *unstructured.Unstructured
	pristine *unstructured.Unstructured
	fields   map[string]*FieldOrigin
	seq      int
	limits   GraphLimits

	fieldTruncated bool
	opTruncated    bool
	// removed holds tombstone nodes of deleted list elements so deletion evidence survives.
	removed []*FieldOrigin
}

func newOriginTracer(target *unstructured.Unstructured, cluster *clusterv1alpha1.Cluster, pristine *unstructured.Unstructured) *originTracer {
	digest := ""
	if b, err := pristine.MarshalJSON(); err == nil {
		sum := sha256.Sum256(b)
		digest = hex.EncodeToString(sum[:])
	}
	graph := &OverrideOriginGraph{
		SchemaVersion: OverrideOriginGraphSchemaVersion,
		Resource: ResourceRef{
			APIVersion: target.GetAPIVersion(),
			Kind:       target.GetKind(),
			Namespace:  target.GetNamespace(),
			Name:       target.GetName(),
			UID:        string(target.GetUID()),
		},
		Cluster:        ClusterRef{Name: cluster.GetName(), UID: string(cluster.UID)},
		TemplateDigest: digest,
		Limits: GraphLimits{
			MaxFields:      defaultMaxGraphFields,
			MaxOperations:  defaultMaxGraphOperations,
			MaxOutputBytes: defaultMaxGraphOutputBytes,
		},
	}
	return &originTracer{
		graph:    graph,
		target:   target,
		pristine: pristine,
		fields:   make(map[string]*FieldOrigin),
		limits:   graph.Limits,
	}
}

func (g *OverrideOriginGraph) addReason(reason string) {
	for _, r := range g.TruncationReasons {
		if r == reason {
			return
		}
	}
	g.TruncationReasons = append(g.TruncationReasons, reason)
}

func (t *originTracer) bindPolicy(ref PolicyRef) {
	for _, p := range t.graph.Policies {
		if ref.UID != "" && p.UID == ref.UID {
			return
		}
		if ref.UID == "" && p.Kind == ref.Kind && p.Namespace == ref.Namespace && p.Name == ref.Name {
			return
		}
	}
	t.graph.Policies = append(t.graph.Policies, ref)
}

func policyRefOf(p policyOverriders) PolicyRef {
	return PolicyRef{
		Kind:       p.policyKind,
		Namespace:  p.namespace,
		Name:       p.name,
		UID:        string(p.uid),
		Generation: p.generation,
	}
}

func policyKindOf(policy GeneralOverridePolicy) string {
	switch policy.(type) {
	case *policyv1alpha1.ClusterOverridePolicy:
		return policyv1alpha1.ResourceKindClusterOverridePolicy
	default:
		return policyv1alpha1.ResourceKindOverridePolicy
	}
}

// applyPolicyOverridersWithTracing mirrors applyPolicyOverriders exactly, including the overrider
// ordering (image -> command -> args -> labels -> annotations -> field -> plaintext), but routes
// every generated overrideOption through the tracer in addition to the real applier.
func (t *originTracer) applyPolicyOverridersWithTracing(rawObj *unstructured.Unstructured, overriders policyv1alpha1.Overriders, ref PolicyRef) error {
	t.bindPolicy(ref)

	for i := range overriders.ImageOverrider {
		patches, err := buildPatches(rawObj, &overriders.ImageOverrider[i])
		if err != nil {
			klog.Errorf("Build patches with imageOverrides err: %v", err)
			return err
		}
		for _, patch := range patches {
			meta := traceMeta{
				policy:         ref,
				kind:           originKindImage,
				index:          i,
				imageComponent: string(overriders.ImageOverrider[i].Component),
				imageOperator:  string(overriders.ImageOverrider[i].Operator),
			}
			if err = t.applyOuterPatch(rawObj, patch, meta); err != nil {
				return err
			}
		}
	}

	for i := range overriders.CommandOverrider {
		patches, err := buildCommandArgsPatches(CommandString, rawObj, &overriders.CommandOverrider[i])
		if err != nil {
			return err
		}
		for _, patch := range patches {
			if err = t.applyOuterPatch(rawObj, patch, traceMeta{policy: ref, kind: originKindCommand, index: i}); err != nil {
				return err
			}
		}
	}

	for i := range overriders.ArgsOverrider {
		patches, err := buildCommandArgsPatches(ArgsString, rawObj, &overriders.ArgsOverrider[i])
		if err != nil {
			return err
		}
		for _, patch := range patches {
			if err = t.applyOuterPatch(rawObj, patch, traceMeta{policy: ref, kind: originKindArgs, index: i}); err != nil {
				return err
			}
		}
	}

	for i := range overriders.LabelsOverrider {
		patches := buildLabelAnnotationOverriderPatches(rawObj, overriders.LabelsOverrider[i], []string{"metadata", "labels"})
		for _, patch := range patches {
			if err := t.applyOuterPatch(rawObj, patch, traceMeta{policy: ref, kind: originKindLabels, index: i}); err != nil {
				return err
			}
		}
	}

	for i := range overriders.AnnotationsOverrider {
		patches := buildLabelAnnotationOverriderPatches(rawObj, overriders.AnnotationsOverrider[i], []string{"metadata", "annotations"})
		for _, patch := range patches {
			if err := t.applyOuterPatch(rawObj, patch, traceMeta{policy: ref, kind: originKindAnnotations, index: i}); err != nil {
				return err
			}
		}
	}

	if err := t.applyFieldOverridersWithTracing(rawObj, overriders.FieldOverrider, ref); err != nil {
		return err
	}

	plaintext := parseJSONPatchesByPlaintext(overriders.Plaintext)
	for i, patch := range plaintext {
		if err := t.applyOuterPatch(rawObj, patch, traceMeta{policy: ref, kind: originKindPlaintext, index: i}); err != nil {
			return err
		}
	}
	return nil
}

// applyOuterPatch applies one outer-object patch through the real applier and records its effects.
func (t *originTracer) applyOuterPatch(obj *unstructured.Unstructured, opt overrideOption, meta traceMeta) error {
	if err := validateTracedOp(opt.Op, opt.Path); err != nil {
		return err
	}
	existed := pathExists(obj.Object, opt.Path)
	container := t.resolveContainer(opt.Path)
	sensitive := isSensitivePath(obj.GetKind(), opt.Path)

	// Real application first: if it fails, the error protocol of ApplyOverridePolicies is kept
	// and the caller discards the whole graph. No evidence is recorded for a write that did not
	// happen.
	if err := applyJSONPatch(obj, []overrideOption{opt}); err != nil {
		return err
	}

	shifts, tombstones := t.bookkeepArrayMutation("", opt)
	if len(tombstones) > 0 {
		// An array element was removed: the deletion evidence belongs on the tombstone nodes,
		// never on a field that merely shifted into the vacated index.
		t.seq++
		t.appendTombstoneRecords(tombstones, t.seq, opt, meta, sensitive, container)
		t.appendOperation(t.seq, opt, meta, opt.Path, sensitive, container, shifts)
		return nil
	}
	t.recordWrite(opt, meta, existed, sensitive, container, shifts)
	return nil
}

// appendTombstoneRecords attaches the terminal remove record to every node of a deleted element.
func (t *originTracer) appendTombstoneRecords(tombstones []*FieldOrigin, seq int, opt overrideOption, meta traceMeta, sensitive bool, container *ContainerIdentity) {
	for _, origin := range tombstones {
		origin.Writes = append(origin.Writes, FieldWriteRecord{
			Sequence:       seq,
			Policy:         meta.policy,
			OverriderKind:  meta.kind,
			OverriderIndex: meta.index,
			Op:             opt.Op,
			SourceLevel:    SourceLevelPolicy,
			State:          writeStateDeleted,
			Container:      container,
			Sensitive:      sensitive,
		})
	}
}

// applyFieldOverridersWithTracing mirrors applyFieldOverriders. The embedded JSON/YAML document is
// patched through the same raw appliers as the legacy path; per-op application yields the exact
// same final bytes as a single batched patch.
func (t *originTracer) applyFieldOverridersWithTracing(rawObj *unstructured.Unstructured, fieldOverriders []policyv1alpha1.FieldOverrider, ref PolicyRef) error {
	if len(fieldOverriders) == 0 {
		return nil
	}
	for index := range fieldOverriders {
		if err := t.applySingleFieldOverrider(rawObj, &fieldOverriders[index], index, ref); err != nil {
			return err
		}
	}
	return nil
}

// applySingleFieldOverrider traces one FieldOverrider against the string field it points at.
func (t *originTracer) applySingleFieldOverrider(rawObj *unstructured.Unstructured, overrider *policyv1alpha1.FieldOverrider, index int, ref PolicyRef) error {
	pointer, err := jsonpointer.New(overrider.FieldPath)
	if err != nil {
		klog.Errorf("Build jsonpointer with overrider's path err: %v", err)
		return err
	}
	res, kind, err := pointer.Get(rawObj.Object)
	if err != nil {
		klog.Errorf("Get value by overrider's path err: %v", err)
		return err
	}
	if kind != reflect.String {
		errMsg := fmt.Sprintf("Get object's value by overrider's path(%s) is not string", overrider.FieldPath)
		klog.Error(errMsg)
		return errors.New(errMsg)
	}

	originKind, current, err := t.decodeEmbeddedDocument(overrider, []byte(res.(string)))
	if err != nil {
		return err
	}
	if originKind == "" {
		// Keep identical to the legacy behavior: with no operation the string field is reset.
		_, setErr := pointer.Set(rawObj.Object, "")
		return setErr
	}

	opCount := len(overrider.JSON)
	if originKind == originKindFieldYAML {
		opCount = len(overrider.YAML)
	}
	for opIndex := 0; opIndex < opCount; opIndex++ {
		current, err = t.applyEmbeddedOperation(rawObj, overrider, originKind, index, opIndex, ref, current)
		if err != nil {
			return err
		}
	}

	outBytes, err := t.encodeEmbeddedDocument(originKind, current)
	if err != nil {
		return err
	}
	if _, err = pointer.Set(rawObj.Object, string(outBytes)); err != nil {
		return err
	}

	// Outer evidence: the string field pointed at by FieldPath was itself replaced.
	outerExisted := pathExists(t.pristine.Object, overrider.FieldPath)
	outerOpt := overrideOption{Op: string(policyv1alpha1.OverriderOpReplace), Path: overrider.FieldPath, Value: string(outBytes)}
	t.recordWrite(outerOpt, traceMeta{
		policy: ref,
		kind:   originKind,
		index:  index,
	}, outerExisted, isSensitivePath(rawObj.GetKind(), overrider.FieldPath), t.resolveContainer(overrider.FieldPath), nil)
	return nil
}

// decodeEmbeddedDocument converts the embedded document into raw JSON and reports its overrider kind.
// An empty kind means the FieldOverrider carries neither JSON nor YAML operations.
func (t *originTracer) decodeEmbeddedDocument(overrider *policyv1alpha1.FieldOverrider, dataBytes []byte) (string, []byte, error) {
	switch {
	case len(overrider.YAML) > 0:
		current, err := yaml.YAMLToJSON(dataBytes)
		if err != nil {
			klog.ErrorS(err, "Failed to convert yaml to json")
			return "", nil, err
		}
		return originKindFieldYAML, current, nil
	case len(overrider.JSON) > 0:
		return originKindFieldJSON, dataBytes, nil
	default:
		return "", nil, nil
	}
}

// applyEmbeddedOperation applies and traces one JSON/YAML operation inside the embedded document.
func (t *originTracer) applyEmbeddedOperation(rawObj *unstructured.Unstructured, overrider *policyv1alpha1.FieldOverrider, originKind string, index, opIndex int, ref PolicyRef, current []byte) ([]byte, error) {
	var (
		operator policyv1alpha1.OverriderOperator
		subPath  string
		rawValue apiextensionsv1.JSON
	)
	if originKind == originKindFieldYAML {
		operator = overrider.YAML[opIndex].Operator
		subPath = overrider.YAML[opIndex].SubPath
		rawValue = overrider.YAML[opIndex].Value
	} else {
		operator = overrider.JSON[opIndex].Operator
		subPath = overrider.JSON[opIndex].SubPath
		rawValue = overrider.JSON[opIndex].Value
	}
	opt := overrideOption{Op: string(operator), Path: subPath, Value: normalizeJSONValue(rawValue)}
	if err := validateTracedOp(opt.Op, subPath); err != nil {
		return nil, err
	}
	existed := pathExistsInRaw(current, subPath)
	applied, err := applyRawJSONPatch(current, []overrideOption{opt})
	if err != nil {
		klog.Errorf("Error applying raw field patch: %v", err)
		return nil, err
	}

	meta := traceMeta{
		policy:        ref,
		kind:          originKind,
		index:         index,
		embeddedField: overrider.FieldPath,
		embeddedSub:   subPath,
		inputLevel:    SourceLevelInput,
	}
	if originKind == originKindFieldYAML {
		// YAML is parsed as an opaque document; no source line numbers are available,
		// so inherited fragments can only be attributed at InputLevel.
		meta.inputLevel = SourceLevelInputLevel
	}
	sensitive := isSensitivePath(rawObj.GetKind(), overrider.FieldPath)
	shifts, tombstones := t.bookkeepArrayMutation(overrider.FieldPath, opt)
	opPath := overrider.FieldPath + embeddedPathSeparator + subPath
	if len(tombstones) > 0 {
		t.seq++
		t.appendTombstoneRecords(tombstones, t.seq, opt, meta, sensitive, nil)
		t.appendOperation(t.seq, opt, meta, opPath, sensitive, nil, shifts)
		return applied, nil
	}
	t.recordEmbeddedWrite(opt, meta, existed, sensitive, shifts)
	return applied, nil
}

// encodeEmbeddedDocument converts the patched raw JSON back into the on-disk representation.
func (t *originTracer) encodeEmbeddedDocument(originKind string, current []byte) ([]byte, error) {
	if originKind != originKindFieldYAML {
		return current, nil
	}
	outBytes, err := yaml.JSONToYAML(current)
	if err != nil {
		klog.Errorf("Failed to convert json to yaml, error: %v", err)
		return nil, err
	}
	return outBytes, nil
}

// recordWrite records one outer-object patch that was applied successfully.
func (t *originTracer) recordWrite(opt overrideOption, meta traceMeta, existed, sensitive bool, container *ContainerIdentity, shifts []ArrayShift) {
	t.seq++
	origin := t.originFor(opt.Path, meta, existed)
	t.appendWrite(origin, t.seq, opt, meta, sensitive, container, shifts)
}

// recordEmbeddedWrite records one patch inside an embedded JSON/YAML document.
func (t *originTracer) recordEmbeddedWrite(opt overrideOption, meta traceMeta, existed, sensitive bool, shifts []ArrayShift) {
	t.seq++
	key := meta.embeddedField + embeddedPathSeparator + meta.embeddedSub
	origin := t.originFor(key, meta, existed)
	t.appendWrite(origin, t.seq, opt, meta, sensitive, nil, shifts)
}

func (t *originTracer) appendWrite(origin *FieldOrigin, seq int, opt overrideOption, meta traceMeta, sensitive bool, container *ContainerIdentity, shifts []ArrayShift) {
	record := FieldWriteRecord{
		Sequence:       seq,
		Policy:         meta.policy,
		OverriderKind:  meta.kind,
		OverriderIndex: meta.index,
		Op:             opt.Op,
		SourceLevel:    SourceLevelPolicy,
		Container:      container,
		ImageComponent: meta.imageComponent,
		ImageOperator:  meta.imageOperator,
		Sensitive:      sensitive,
	}

	switch opt.Op {
	case string(policyv1alpha1.OverriderOpRemove):
		// A removal is terminal deletion evidence for the previous owner.
		if origin != nil {
			t.markPrevious(origin, seq, writeStateDeleted)
		}
		record.State = writeStateDeleted
	default:
		if origin != nil {
			t.markPrevious(origin, seq, writeStateSuperseded)
		}
		record.State = writeStateActive
		value, summary := t.prepareValue(sensitive, opt.Value)
		record.Value = value
		record.ValueSummary = summary
	}
	if origin != nil {
		origin.Writes = append(origin.Writes, record)
	}

	opPath := opt.Path
	if meta.embeddedField != "" {
		opPath = meta.embeddedField + embeddedPathSeparator + meta.embeddedSub
	}
	t.appendOperation(seq, opt, meta, opPath, sensitive, container, shifts)
}

// markPrevious marks the last active write of a field as superseded or deleted by causeSeq.
func (t *originTracer) markPrevious(origin *FieldOrigin, causeSeq int, state string) {
	for i := len(origin.Writes) - 1; i >= 0; i-- {
		if origin.Writes[i].State == writeStateActive {
			origin.Writes[i].State = state
			seq := causeSeq
			origin.Writes[i].SupersededBy = &seq
			return
		}
	}
}

// originFor returns (creating if needed) the lineage node of a field. When the field budget is
// exhausted it returns nil: operation evidence is still appended, but no new field node is faked.
func (t *originTracer) originFor(key string, meta traceMeta, existed bool) *FieldOrigin {
	if origin, ok := t.fields[key]; ok {
		return origin
	}
	if len(t.fields) >= t.limits.MaxFields {
		if !t.fieldTruncated {
			t.fieldTruncated = true
			t.graph.Truncated = true
			t.graph.addReason(truncationReasonFields)
		}
		return nil
	}
	origin := &FieldOrigin{
		Path:             key,
		ExistedInInput:   existed,
		InputSourceLevel: meta.inputLevel,
	}
	if meta.embeddedField != "" {
		origin.EmbeddedFieldPath = meta.embeddedField
		origin.EmbeddedSubPath = meta.embeddedSub
		origin.InputSourceLevel = meta.inputLevel
	}
	t.fields[key] = origin
	return origin
}

func (t *originTracer) appendOperation(seq int, opt overrideOption, meta traceMeta, opPath string, sensitive bool, container *ContainerIdentity, shifts []ArrayShift) {
	if len(t.graph.Operations) >= t.limits.MaxOperations {
		if !t.opTruncated {
			t.opTruncated = true
			t.graph.Truncated = true
			t.graph.addReason(truncationReasonOperations)
		}
		return
	}
	_, summary := t.prepareValue(sensitive, opt.Value)
	record := OperationRecord{
		Sequence:          seq,
		Policy:            meta.policy,
		OverriderKind:     meta.kind,
		OverriderIndex:    meta.index,
		Op:                opt.Op,
		Path:              opPath,
		Container:         container,
		Sensitive:         sensitive,
		ValueSummary:      summary,
		ShiftedFields:     shifts,
		EmbeddedFieldPath: meta.embeddedField,
		EmbeddedSubPath:   meta.embeddedSub,
	}
	t.graph.Operations = append(t.graph.Operations, record)
}

// bookkeepArrayMutation updates tracked field paths when an add/remove targets a list. It mirrors
// RFC 6901 index semantics: add at index i shifts j>=i up, remove at index i shifts j>i down and
// returns the tombstone nodes of the deleted element. A non-empty tombstone slice tells callers
// the remove evidence is already attached and must not be re-attached to a shifted-in field.
func (t *originTracer) bookkeepArrayMutation(embeddedField string, opt overrideOption) ([]ArrayShift, []*FieldOrigin) {
	tokens := splitPointer(opt.Path)
	if len(tokens) == 0 {
		return nil, nil
	}
	idx, ok := parseArrayIndex(tokens[len(tokens)-1])
	if !ok {
		// "-" appends and does not move existing indices.
		return nil, nil
	}
	arrayTokens := tokens[:len(tokens)-1]
	causeSeq := t.seq + 1

	if opt.Op == string(policyv1alpha1.OverriderOpRemove) {
		elementPath := joinPointer(tokens)
		prefix := elementPath + "/"
		var deleted []string
		var tombstones []*FieldOrigin
		for key, origin := range t.fields {
			if key != elementPath && !strings.HasPrefix(key, prefix) {
				continue
			}
			if embeddedField != "" && !strings.HasPrefix(key, embeddedField+embeddedPathSeparator) {
				continue
			}
			t.markPrevious(origin, causeSeq, writeStateDeleted)
			origin.FinalOwner = nil
			origin.Removed = true
			t.removed = append(t.removed, origin)
			tombstones = append(tombstones, origin)
			deleted = append(deleted, key)
		}
		for _, key := range deleted {
			delete(t.fields, key)
		}
		return t.shiftIndices(embeddedField, arrayTokens, idx+1, -1, causeSeq), tombstones
	}

	if opt.Op == string(policyv1alpha1.OverriderOpAdd) {
		return t.shiftIndices(embeddedField, arrayTokens, idx, +1, causeSeq), nil
	}
	return nil, nil
}

// shiftIndices moves every tracked field under the given list whose index meets the threshold.
func (t *originTracer) shiftIndices(embeddedField string, arrayTokens []string, threshold, delta, causeSeq int) []ArrayShift {
	arraySubPath := joinPointerTokens(arrayTokens)
	arrayPath := arraySubPath
	if embeddedField != "" {
		arrayPath = embeddedField + embeddedPathSeparator + arraySubPath
	}
	type rekey struct {
		from, to       string
		fromIdx, toIdx int
	}
	var rekeys []rekey
	for key, origin := range t.fields {
		subPath := key
		if embeddedField != "" {
			prefix := embeddedField + embeddedPathSeparator
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			subPath = strings.TrimPrefix(key, prefix)
		}
		tokens := splitPointer(subPath)
		if len(tokens) <= len(arrayTokens) || joinPointerTokens(tokens[:len(arrayTokens)]) != arraySubPath {
			continue
		}
		idx, err := strconv.Atoi(tokens[len(arrayTokens)])
		if err != nil || idx < threshold {
			continue
		}
		newTokens := append(append([]string{}, tokens[:len(arrayTokens)]...), strconv.Itoa(idx+delta))
		newTokens = append(newTokens, tokens[len(arrayTokens)+1:]...)
		newSub := joinPointerTokens(newTokens)
		newKey := newSub
		if embeddedField != "" {
			newKey = embeddedField + embeddedPathSeparator + newSub
		}
		rekeys = append(rekeys, rekey{from: key, to: newKey, fromIdx: idx, toIdx: idx + delta})
		origin.Path = newKey
	}
	// Apply rekeys in an order that cannot overwrite an unprocessed entry.
	sort.Slice(rekeys, func(i, j int) bool {
		if delta > 0 {
			return rekeys[i].fromIdx > rekeys[j].fromIdx
		}
		return rekeys[i].fromIdx < rekeys[j].fromIdx
	})
	shifts := make([]ArrayShift, 0, len(rekeys))
	for _, r := range rekeys {
		if r.from == r.to {
			continue
		}
		t.fields[r.to] = t.fields[r.from]
		delete(t.fields, r.from)
		shifts = append(shifts, ArrayShift{
			ArrayPath: arrayPath,
			FieldPath: r.to,
			FromIndex: r.fromIdx,
			ToIndex:   r.toIdx,
		})
	}
	sort.Slice(shifts, func(i, j int) bool { return shifts[i].FieldPath < shifts[j].FieldPath })
	return shifts
}

// finalize resolves final owners from the real recorded writes and produces the graph payload.
func (t *originTracer) finalize() *OverrideOriginGraph {
	keys := make([]string, 0, len(t.fields))
	for key := range t.fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	allOrigins := make([]*FieldOrigin, 0, len(t.fields)+len(t.removed))
	for _, key := range keys {
		allOrigins = append(allOrigins, t.fields[key])
	}
	allOrigins = append(allOrigins, t.removed...)

	fields := make([]FieldOrigin, 0, len(allOrigins))
	for _, origin := range allOrigins {
		owner := -1
		for _, write := range origin.Writes {
			if write.State == writeStateActive {
				owner = write.Sequence
			}
		}
		if owner >= 0 {
			origin.FinalOwner = &owner
		} else {
			origin.FinalOwner = nil
		}
		fields = append(fields, *origin)
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Path < fields[j].Path })
	t.graph.Fields = fields
	return t.graph
}

// resolveContainer resolves the real container identity behind a container-scoped path. It runs
// before the patch is applied, so the name is still correct for removals.
func (t *originTracer) resolveContainer(path string) *ContainerIdentity {
	tokens := splitPointer(path)
	for i := 0; i+1 < len(tokens); i++ {
		if tokens[i] != "containers" && tokens[i] != "initContainers" {
			continue
		}
		idx, err := strconv.Atoi(tokens[i+1])
		if err != nil {
			continue
		}
		identity := &ContainerIdentity{Type: tokens[i], Index: idx}
		if pointer, err := jsonpointer.New(joinPointerTokens(tokens[:i+2]) + "/name"); err == nil {
			if name, kind, err := pointer.Get(t.target.Object); err == nil && kind == reflect.String {
				identity.Name = name.(string)
			}
		}
		return identity
	}
	return nil
}

// prepareValue returns the storable value and a non-reversible summary. Sensitive fields keep only
// the summary; non-sensitive values are capped so full-text configuration is never retained.
func (t *originTracer) prepareValue(sensitive bool, value any) (any, string) {
	normalized := normalizeJSONValue(value)
	raw, err := json.Marshal(normalized)
	if err != nil {
		return nil, "unmarshallable-value"
	}
	sum := sha256.Sum256(raw)
	summary := fmt.Sprintf("type=%s len=%d sha256=%s", jsonTypeName(normalized), len(raw), hex.EncodeToString(sum[:])[:12])
	if sensitive {
		return nil, summary
	}
	if len(raw) > maxValuePreviewBytes {
		return string(raw[:maxValuePreviewBytes]) + "...(truncated)", summary
	}
	return normalized, summary
}

// isSensitivePath classifies fields whose raw value must never appear in the graph.
func isSensitivePath(kind, path string) bool {
	tokens := splitPointer(path)
	if kind == "Secret" {
		for _, token := range tokens {
			if token == "data" || token == "stringData" {
				return true
			}
		}
	}
	for _, token := range tokens {
		lower := strings.ToLower(token)
		switch lower {
		case "password", "passwd", "token", "secret", "credential", "credentials", "auth":
			return true
		}
		if strings.Contains(lower, "password") || strings.Contains(lower, "credential") || strings.Contains(lower, "secret") {
			return true
		}
	}
	return false
}

// validateTracedOp keeps the original error protocol for unsupported operations. The validated
// override APIs only emit add/remove/replace; move/copy/test are rejected instead of producing
// fabricated evidence.
func validateTracedOp(op, path string) error {
	switch policyv1alpha1.OverriderOperator(op) {
	case policyv1alpha1.OverriderOpAdd, policyv1alpha1.OverriderOpRemove, policyv1alpha1.OverriderOpReplace:
		return nil
	default:
		return fmt.Errorf("unsupported override operation %q for path %q", op, path)
	}
}

func pathExists(doc any, path string) bool {
	if path == "" {
		return true
	}
	pointer, err := jsonpointer.New(path)
	if err != nil {
		return false
	}
	_, _, err = pointer.Get(doc)
	return err == nil
}

func pathExistsInRaw(rawJSON []byte, path string) bool {
	var doc any
	if err := json.Unmarshal(rawJSON, &doc); err != nil {
		return false
	}
	return pathExists(doc, path)
}

// normalizeJSONValue decodes apiextensionsv1.JSON wrappers so the graph stores natural JSON values.
func normalizeJSONValue(value any) any {
	switch v := value.(type) {
	case apiextensionsv1.JSON:
		var decoded any
		if len(v.Raw) == 0 {
			return nil
		}
		if err := json.Unmarshal(v.Raw, &decoded); err != nil {
			return string(v.Raw)
		}
		return decoded
	case *apiextensionsv1.JSON:
		if v == nil {
			return nil
		}
		return normalizeJSONValue(*v)
	default:
		return value
	}
}

func jsonTypeName(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "boolean"
	case json.Number, float64, int, int64:
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", value)
	}
}

// splitPointer decodes an RFC6901 JSON pointer into unescaped tokens.
func splitPointer(path string) []string {
	if path == "" || path == "/" {
		return nil
	}
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.Split(trimmed, "/")
	for i := range parts {
		parts[i] = decodePointerToken(parts[i])
	}
	return parts
}

func decodePointerToken(token string) string {
	token = strings.ReplaceAll(token, "~1", "/")
	token = strings.ReplaceAll(token, "~0", "~")
	return token
}

func encodePointerToken(token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	token = strings.ReplaceAll(token, "/", "~1")
	return token
}

func joinPointerTokens(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	encoded := make([]string, len(tokens))
	for i, token := range tokens {
		encoded[i] = encodePointerToken(token)
	}
	return "/" + strings.Join(encoded, "/")
}

func joinPointer(tokens []string) string {
	return joinPointerTokens(tokens)
}

func parseArrayIndex(token string) (int, bool) {
	if token == "-" {
		return 0, false
	}
	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0, false
	}
	return idx, true
}

// MarshalJSON enforces the output byte budget by progressively shedding payload detail. The
// bindings and field lineage evidence are preserved as long as possible; truncation is always
// marked explicitly.
func (g *OverrideOriginGraph) MarshalJSON() ([]byte, error) {
	type plainGraph OverrideOriginGraph
	limit := g.Limits.MaxOutputBytes
	if limit <= 0 {
		limit = defaultMaxGraphOutputBytes
	}

	b, err := json.Marshal((*plainGraph)(g))
	if err != nil || len(b) <= limit {
		return b, err
	}

	// Work on an independent payload so trimming never mutates the in-memory graph.
	clone := *g
	clone.Truncated = true
	clone.TruncationReasons = append(append([]string{}, g.TruncationReasons...), truncationReasonOutput)

	// Step 1: drop the operation log, which is the bulkiest section.
	clone.Operations = nil
	if b, err = json.Marshal((*plainGraph)(&clone)); err == nil && len(b) <= limit {
		return b, nil
	}

	// Step 2: strip values from write records, keep summaries and lineage.
	clone.Fields = stripFieldValues(g.Fields)
	if b, err = json.Marshal((*plainGraph)(&clone)); err == nil && len(b) <= limit {
		return b, nil
	}

	// Step 3: retain only fields that still have a final owner.
	kept := make([]FieldOrigin, 0, len(clone.Fields))
	for _, field := range clone.Fields {
		if field.FinalOwner != nil {
			kept = append(kept, field)
		}
	}
	clone.Fields = kept
	if b, err = json.Marshal((*plainGraph)(&clone)); err == nil && len(b) <= limit {
		return b, nil
	}

	// Step 4: hard cap the number of field nodes, halving until the budget fits.
	for len(clone.Fields) > 1 {
		clone.Fields = clone.Fields[:len(clone.Fields)/2]
		if b, err = json.Marshal((*plainGraph)(&clone)); err == nil && len(b) <= limit {
			return b, nil
		}
	}
	return json.Marshal((*plainGraph)(&clone))
}

// stripFieldValues deep-copies field nodes while removing raw values, preserving summaries.
func stripFieldValues(src []FieldOrigin) []FieldOrigin {
	out := make([]FieldOrigin, len(src))
	for i, field := range src {
		writes := make([]FieldWriteRecord, len(field.Writes))
		for j, write := range field.Writes {
			write.Value = nil
			writes[j] = write
		}
		field.Writes = writes
		if field.FinalOwner != nil {
			owner := *field.FinalOwner
			field.FinalOwner = &owner
		}
		out[i] = field
	}
	return out
}

// ApplyOverridePoliciesWithOriginGraph runs the exact same override pipeline as
// ApplyOverridePolicies on the passed object and additionally returns the field-level origin
// graph. A pristine deep copy is kept for the template digest and input-provenance checks and is
// never mutated. When the feature gate is disabled it behaves as a plain delegation and returns a
// nil graph, keeping the feature default-off with zero behavioral change.
func (o *overrideManagerImpl) ApplyOverridePoliciesWithOriginGraph(rawObj *unstructured.Unstructured, clusterName string) (*AppliedOverrides, *AppliedOverrides, *OverrideOriginGraph, error) {
	if !features.FeatureGate.Enabled(features.OverrideOriginGraph) {
		appliedClusterPolicies, appliedNamespacedPolicies, err := o.ApplyOverridePolicies(rawObj, clusterName)
		return appliedClusterPolicies, appliedNamespacedPolicies, nil, err
	}

	clusterObj := &clusterv1alpha1.Cluster{}
	if err := o.Client.Get(context.TODO(), client.ObjectKey{Name: clusterName}, clusterObj); err != nil {
		klog.Errorf("Failed to get member cluster: %s, error: %v", clusterName, err)
		return nil, nil, nil, err
	}

	pristine := rawObj.DeepCopy()
	tracer := newOriginTracer(rawObj, clusterObj, pristine)

	appliedClusterOverrides, err := o.applyClusterOverridesWithTracing(rawObj, clusterObj, tracer)
	if err != nil {
		klog.Errorf("Failed to apply cluster override policies. error: %v", err)
		return nil, nil, nil, err
	}

	var appliedNamespacedOverrides *AppliedOverrides
	// For namespace scoped resources, should apply override policies under the same namespace.
	// No matter the resources propagated by ClusterPropagationPolicy or PropagationPolicy.
	if len(rawObj.GetNamespace()) > 0 {
		appliedNamespacedOverrides, err = o.applyNamespacedOverridesWithTracing(rawObj, clusterObj, tracer)
		if err != nil {
			klog.Errorf("Failed to apply namespaced override policies. error: %v", err)
			return nil, nil, nil, err
		}
	}

	return appliedClusterOverrides, appliedNamespacedOverrides, tracer.finalize(), nil
}

// applyClusterOverridesWithTracing is the traced counterpart of applyClusterOverrides. Policy
// selection, selector matching, implicit-priority sorting and name sorting are reused unchanged.
func (o *overrideManagerImpl) applyClusterOverridesWithTracing(rawObj *unstructured.Unstructured, cluster *clusterv1alpha1.Cluster, tracer *originTracer) (*AppliedOverrides, error) {
	policyList := &policyv1alpha1.ClusterOverridePolicyList{}
	if err := o.Client.List(context.TODO(), policyList, &client.ListOptions{UnsafeDisableDeepCopy: new(true)}); err != nil {
		klog.Errorf("Failed to list cluster override policies, error: %v", err)
		return nil, err
	}
	if len(policyList.Items) == 0 {
		return nil, nil
	}

	items := make([]GeneralOverridePolicy, 0, len(policyList.Items))
	for i := range policyList.Items {
		items = append(items, &policyList.Items[i])
	}
	matchingPolicyOverriders := o.getOverridersFromOverridePolicies(items, rawObj, cluster)
	if len(matchingPolicyOverriders) == 0 {
		klog.V(2).Infof("No cluster override policy for resource: %s/%s", rawObj.GetNamespace(), rawObj.GetName())
		return nil, nil
	}

	appliedList := &AppliedOverrides{}
	for _, p := range matchingPolicyOverriders {
		ref := policyRefOf(p)
		if err := tracer.applyPolicyOverridersWithTracing(rawObj, p.overriders, ref); err != nil {
			klog.Errorf("Failed to apply cluster overrides(%s) for resource(%s/%s), error: %v", p.name, rawObj.GetNamespace(), rawObj.GetName(), err)
			o.EventRecorder.Eventf(rawObj, corev1.EventTypeWarning, events.EventReasonApplyOverridePolicyFailed, "Apply cluster override policy(%s) for cluster(%s) failed.", p.name, cluster.Name)
			return nil, err
		}
		klog.V(2).Infof("Applied cluster overrides(%s) for resource(%s/%s)", p.name, rawObj.GetNamespace(), rawObj.GetName())
		o.EventRecorder.Eventf(rawObj, corev1.EventTypeNormal, events.EventReasonApplyOverridePolicySucceed, "Apply cluster override policy(%s) for cluster(%s) succeed.", p.name, cluster.Name)
		appliedList.Add(p.name, p.overriders)
	}
	return appliedList, nil
}

// applyNamespacedOverridesWithTracing is the traced counterpart of applyNamespacedOverrides.
func (o *overrideManagerImpl) applyNamespacedOverridesWithTracing(rawObj *unstructured.Unstructured, cluster *clusterv1alpha1.Cluster, tracer *originTracer) (*AppliedOverrides, error) {
	policyList := &policyv1alpha1.OverridePolicyList{}
	if err := o.Client.List(context.TODO(), policyList, &client.ListOptions{Namespace: rawObj.GetNamespace(), UnsafeDisableDeepCopy: new(true)}); err != nil {
		klog.Errorf("Failed to list override policies from namespace: %s, error: %v", rawObj.GetNamespace(), err)
		return nil, err
	}
	if len(policyList.Items) == 0 {
		return nil, nil
	}

	items := make([]GeneralOverridePolicy, 0, len(policyList.Items))
	for i := range policyList.Items {
		items = append(items, &policyList.Items[i])
	}
	matchingPolicyOverriders := o.getOverridersFromOverridePolicies(items, rawObj, cluster)
	if len(matchingPolicyOverriders) == 0 {
		klog.V(2).Infof("No override policy for resource(%s/%s)", rawObj.GetNamespace(), rawObj.GetName())
		return nil, nil
	}

	appliedList := &AppliedOverrides{}
	for _, p := range matchingPolicyOverriders {
		ref := policyRefOf(p)
		if err := tracer.applyPolicyOverridersWithTracing(rawObj, p.overriders, ref); err != nil {
			klog.Errorf("Failed to apply overrides(%s/%s) for resource(%s/%s), error: %v", p.namespace, p.name, rawObj.GetNamespace(), rawObj.GetName(), err)
			o.EventRecorder.Eventf(rawObj, corev1.EventTypeWarning, events.EventReasonApplyOverridePolicyFailed, "Apply override policy(%s/%s) for cluster(%s) failed.", p.namespace, p.name, cluster.Name)
			return nil, err
		}
		klog.V(2).Infof("Applied overrides(%s/%s) for resource(%s/%s)", p.namespace, p.name, rawObj.GetNamespace(), rawObj.GetName())
		o.EventRecorder.Eventf(rawObj, corev1.EventTypeNormal, events.EventReasonApplyOverridePolicySucceed, "Apply override policy(%s/%s) for cluster(%s) succeed.", p.namespace, p.name, cluster.Name)
		appliedList.Add(p.name, p.overriders)
	}
	return appliedList, nil
}
