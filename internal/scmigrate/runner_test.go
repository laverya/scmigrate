package scmigrate

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestAffinityForNodePinsWithMatchField(t *testing.T) {
	affinity := affinityForNode("node-a")
	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 {
		t.Fatalf("expected one node selector term, got %d", len(terms))
	}
	fields := terms[0].MatchFields
	if len(fields) != 1 {
		t.Fatalf("expected one match field, got %d", len(fields))
	}
	if fields[0].Key != "metadata.name" {
		t.Fatalf("expected metadata.name key, got %q", fields[0].Key)
	}
	if fields[0].Operator != corev1.NodeSelectorOpIn {
		t.Fatalf("expected In operator, got %q", fields[0].Operator)
	}
	if len(fields[0].Values) != 1 || fields[0].Values[0] != "node-a" {
		t.Fatalf("expected node-a pin, got %#v", fields[0].Values)
	}
}

func TestStateBeforeOrdersResumablePhases(t *testing.T) {
	if !stateBefore("", StatePrepared) {
		t.Fatal("empty state should be before prepared")
	}
	if !stateBefore(StateInitialSynced, StateFinalSynced) {
		t.Fatal("initial sync should be before final sync")
	}
	if stateBefore(StateRestored, StateCutover) {
		t.Fatal("restored state should not be before cutover")
	}
	if stateBefore(StateFinalSynced, StateInitialSynced) {
		t.Fatal("final sync should not be before initial sync")
	}
}
