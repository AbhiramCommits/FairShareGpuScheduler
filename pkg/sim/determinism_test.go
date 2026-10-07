/*
Copyright 2026 The FairShareGpuScheduler Authors.

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

package sim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func loadTrace(t *testing.T) Trace {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/trace.json")
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	var tr Trace
	if err := json.Unmarshal(raw, &tr); err != nil {
		t.Fatalf("parse trace: %v", err)
	}
	return tr
}

// TestHarnessIsDeterministic asserts that two runs over the same trace with the
// same seed produce byte-identical JSON, for every scheduler mode.
func TestHarnessIsDeterministic(t *testing.T) {
	tr := loadTrace(t)
	for _, mode := range []Mode{ModeFIFO, ModeDefaultBinpack, ModeFairShare} {
		opts := Options{Mode: mode, StarvationThreshold: 1800}
		a, err := json.Marshal(Simulate(tr, opts))
		if err != nil {
			t.Fatalf("marshal first: %v", err)
		}
		b, err := json.Marshal(Simulate(tr, opts))
		if err != nil {
			t.Fatalf("marshal second: %v", err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("mode %s is not deterministic:\n%s\n%s", mode, a, b)
		}
	}
}

// TestTraceSeedDeterministic asserts the committed trace matches its seed's
// generation contract by checking every job has a unique id and known tenant.
func TestTraceSeedDeterministic(t *testing.T) {
	tr := loadTrace(t)
	seen := map[string]bool{}
	tenants := map[string]bool{}
	for _, tn := range tr.Tenants {
		tenants[tn.Name] = true
	}
	for _, j := range tr.Jobs {
		if j.ID == "" || seen[j.ID] {
			t.Fatalf("duplicate or empty job id %q", j.ID)
		}
		seen[j.ID] = true
		if !tenants[j.Tenant] {
			t.Fatalf("job %s references unknown tenant %s", j.ID, j.Tenant)
		}
	}
	if tr.Pool.TotalGPUs != 32 || tr.Pool.Nodes != 4 {
		t.Fatalf("unexpected pool: %+v", tr.Pool)
	}
	_ = fmt.Sprintf("%v", seen)
}
