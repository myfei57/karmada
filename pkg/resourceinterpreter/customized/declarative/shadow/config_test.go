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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
)

func validRule() Rule {
	return Rule{
		APIVersion:      testAPIVersion,
		Kind:            testKind,
		Operation:       "GetReplicas",
		CandidateScript: "function GetReplicas(o) return 1, nil end",
	}
}

func TestConfigValidation(t *testing.T) {
	t.Run("accepts valid config", func(t *testing.T) {
		cfg := testConfig(validRule())
		assert.NoError(t, cfg.Validate())
	})

	t.Run("rejects out of range sampling rate", func(t *testing.T) {
		cfg := testConfig(validRule())
		cfg.SamplingRate = 1.5
		assert.Error(t, cfg.Validate())
	})

	t.Run("rejects disallowed operation", func(t *testing.T) {
		cfg := testConfig(validRule())
		cfg.Rules[0].Operation = "Retain"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed for shadow interpretation")
	})

	t.Run("rejects empty candidate script", func(t *testing.T) {
		cfg := testConfig(validRule())
		cfg.Rules[0].CandidateScript = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("rejects allowed diff fields outside ReviseReplica", func(t *testing.T) {
		cfg := testConfig(validRule())
		cfg.Rules[0].AllowedDiffFields = []string{"spec.replicas"}
		assert.Error(t, cfg.Validate())
	})

	t.Run("rejects duplicate rules", func(t *testing.T) {
		cfg := testConfig(validRule(), validRule())
		assert.Error(t, cfg.Validate())
	})

	t.Run("rejects malformed api version", func(t *testing.T) {
		cfg := testConfig(validRule())
		cfg.Rules[0].APIVersion = "group/version/kind"
		assert.Error(t, cfg.Validate())
	})
}

func TestConfigDefaults(t *testing.T) {
	cfg := &Config{Enabled: true}
	cfg.SetDefaults()
	assert.Equal(t, defaultQueueCapacity, cfg.QueueCapacity)
	assert.Equal(t, defaultWorkers, cfg.Workers)
	assert.Equal(t, defaultSamplingRate, cfg.SamplingRate)
	assert.Equal(t, defaultMaxInputBytes, cfg.MaxInputBytes)
	assert.Equal(t, defaultMaxQueuedInputBytes, cfg.MaxQueuedInputBytes)
	assert.Equal(t, metav1.Duration{Duration: time.Second}, cfg.ExecutionTimeout)
}

func TestCompileAssignsScriptVersion(t *testing.T) {
	snap, err := testConfig(validRule()).compile(7)
	require.NoError(t, err)
	assert.Equal(t, int64(7), snap.generation)
	rule := snap.rules[ruleKey{
		gvk:       testObject().GroupVersionKind(),
		operation: configv1alpha1.InterpreterOperationInterpretReplica,
	}]
	require.NotNil(t, rule)
	assert.Len(t, rule.version, digestLength)
	assert.NotEmpty(t, rule.script)
}

func TestCompileDisabledSnapshot(t *testing.T) {
	snap, err := (&Config{Enabled: false}).compile(1)
	require.NoError(t, err)
	assert.False(t, snap.config.Enabled)
	assert.NotZero(t, snap.config.QueueCapacity, "defaults must still be applied when disabled")
	assert.Empty(t, snap.rules)
}

func TestLoadConfigFromFile(t *testing.T) {
	t.Run("empty path yields disabled config", func(t *testing.T) {
		cfg, err := LoadConfigFromFile("")
		require.NoError(t, err)
		assert.False(t, cfg.Enabled)
	})

	t.Run("missing file returns error", func(t *testing.T) {
		_, err := LoadConfigFromFile(filepath.Join(t.TempDir(), "missing.yaml"))
		assert.Error(t, err)
	})

	t.Run("valid yaml is loaded with defaults", func(t *testing.T) {
		content := `
enabled: true
samplingRate: 0.25
rules:
  - apiVersion: example.io/v1
    kind: Workload
    operation: ReviseReplica
    candidateScript: |
      function ReviseReplica(obj, replica)
        obj.spec.replicas = replica
        return obj
      end
    allowedDiffFields:
      - spec.replicas
`
		path := filepath.Join(t.TempDir(), "shadow.yaml")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

		cfg, err := LoadConfigFromFile(path)
		require.NoError(t, err)
		assert.True(t, cfg.Enabled)
		assert.Equal(t, 0.25, cfg.SamplingRate)
		require.Len(t, cfg.Rules, 1)
		assert.Equal(t, []string{"spec.replicas"}, cfg.Rules[0].AllowedDiffFields)
		assert.NotZero(t, cfg.QueueCapacity)
	})

	t.Run("invalid yaml returns error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.yaml")
		require.NoError(t, os.WriteFile(path, []byte("enabled: ["), 0o600))
		_, err := LoadConfigFromFile(path)
		assert.Error(t, err)
	})
}
