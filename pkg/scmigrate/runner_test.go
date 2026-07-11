package scmigrate

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSyncPodFailureIncludesTerminationReason(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sync"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name:  "rclone",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", Message: "copy failed"}},
		}}},
	}
	err := syncPodFailure(pod)
	if !strings.Contains(err.Error(), "container rclone: copy failed") {
		t.Fatalf("syncPodFailure() = %v, want container detail", err)
	}
}

func TestSyncPodNamePreservesUIDHashWhenTruncated(t *testing.T) {
	prefix := strings.Repeat("a", 61)
	first := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: prefix + "-x", UID: types.UID("uid-one")}}
	second := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: prefix + "-y", UID: types.UID("uid-two")}}

	firstName := syncPodName(first, SyncPhaseInitial)
	secondName := syncPodName(second, SyncPhaseInitial)
	if len(firstName) > 63 || len(secondName) > 63 {
		t.Fatalf("sync pod names exceed DNS label limit: %q %q", firstName, secondName)
	}
	if firstName == secondName {
		t.Fatalf("distinct PVC UIDs produced the same sync pod name %q", firstName)
	}
	if !strings.HasSuffix(firstName, shortHash(string(first.UID))) || !strings.HasSuffix(secondName, shortHash(string(second.UID))) {
		t.Fatalf("sync pod names do not preserve UID hashes: %q %q", firstName, secondName)
	}
}

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
	before, err := stateBefore("", StatePrepared)
	if err != nil {
		t.Fatal(err)
	}
	if !before {
		t.Fatal("empty state should be before prepared")
	}
	before, err = stateBefore(StateInitialSynced, StateFinalSynced)
	if err != nil {
		t.Fatal(err)
	}
	if !before {
		t.Fatal("initial sync should be before final sync")
	}
	before, err = stateBefore(StateRestored, StateCutover)
	if err != nil {
		t.Fatal(err)
	}
	if before {
		t.Fatal("restored state should not be before cutover")
	}
	before, err = stateBefore(StateFinalSynced, StateInitialSynced)
	if err != nil {
		t.Fatal(err)
	}
	if before {
		t.Fatal("final sync should not be before initial sync")
	}
}

func TestStateBeforeRejectsUnknownState(t *testing.T) {
	if _, err := stateBefore("mystery", StatePrepared); err == nil {
		t.Fatal("stateBefore should reject unknown current states")
	}
	if _, err := stateBefore(StatePrepared, "mystery"); err == nil {
		t.Fatal("stateBefore should reject unknown target states")
	}
}
