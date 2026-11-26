package main

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

// TestTemplateSpec checks template against testdata/template-ids.json, a copy of
// @node-toolkit/core's spec/template-ids.json: the TS package is the spec for template ids, and
// this must give the same id for every program there. Update the copy with the package.
func TestTemplateSpec(t *testing.T) {
	raw, err := os.ReadFile("testdata/template-ids.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Vectors []struct{ Name, Program, ID string }
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	if len(spec.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range spec.Vectors {
		p, err := hex.DecodeString(v.Program)
		if err != nil {
			t.Fatalf("%s: %v", v.Name, err)
		}
		if got := template(p); got != v.ID {
			t.Errorf("%s: template = %s, want %s", v.Name, got, v.ID)
		}
	}
}
