package scmigrate

import (
	"context"
	"io"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

func TestExcludeNodeFromAffinityCreatesRequiredMatchField(t *testing.T) {
	affinity := excludeNodeFromAffinity(nil, "node-a")

	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 {
		t.Fatalf("expected one selector term, got %d", len(terms))
	}
	fields := terms[0].MatchFields
	if len(fields) != 1 {
		t.Fatalf("expected one match field, got %d", len(fields))
	}
	if fields[0].Key != "metadata.name" {
		t.Fatalf("expected metadata.name key, got %q", fields[0].Key)
	}
	if fields[0].Operator != corev1.NodeSelectorOpNotIn {
		t.Fatalf("expected NotIn operator, got %q", fields[0].Operator)
	}
	if len(fields[0].Values) != 1 || fields[0].Values[0] != "node-a" {
		t.Fatalf("expected node-a exclusion, got %#v", fields[0].Values)
	}
}

func TestExcludeNodeFromAffinityPreservesOrTerms(t *testing.T) {
	in := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "disk",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"ssd"},
					}}},
					{MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "zone",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"b"},
					}}},
				},
			},
		},
	}

	affinity := excludeNodeFromAffinity(in, "node-b")
	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 2 {
		t.Fatalf("expected two selector terms, got %d", len(terms))
	}
	for i, term := range terms {
		if len(term.MatchExpressions) != 1 {
			t.Fatalf("term %d lost match expressions: %#v", i, term.MatchExpressions)
		}
		if len(term.MatchFields) != 1 {
			t.Fatalf("term %d expected one match field, got %d", i, len(term.MatchFields))
		}
		if term.MatchFields[0].Operator != corev1.NodeSelectorOpNotIn || term.MatchFields[0].Values[0] != "node-b" {
			t.Fatalf("term %d has wrong node exclusion: %#v", i, term.MatchFields[0])
		}
	}
	if len(in.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields) != 0 {
		t.Fatal("input affinity was mutated")
	}
}

func TestPodReadyRequiresRunningReadyCondition(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if podReady(pod) {
		t.Fatal("pod without Ready condition should not be ready")
	}

	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if !podReady(pod) {
		t.Fatal("running pod with Ready=True should be ready")
	}

	pod.Status.Phase = corev1.PodSucceeded
	if podReady(pod) {
		t.Fatal("non-running pod should not be ready")
	}
}

func TestQuiesceDaemonSetDryRunRecordsOriginalScheduling(t *testing.T) {
	runner := &Runner{opts: Options{DryRun: true}, client: fake.NewSimpleClientset(), out: io.Discard}
	affinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      "storage",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"local"},
				}}}},
			},
		},
	}
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default"},
		Spec: appsv1.DaemonSetSpec{
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.RollingUpdateDaemonSetStrategyType},
			Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{Affinity: affinity}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-node-a", Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
	}

	record, err := runner.quiesceDaemonSet(testContext(t), pod, ds)
	if err != nil {
		t.Fatalf("quiesceDaemonSet returned error: %v", err)
	}
	if record.Kind != "DaemonSet" || record.Name != "agent" || record.NodeName != "node-a" {
		t.Fatalf("unexpected record identity: %#v", record)
	}
	if record.DaemonSetStrategy.Type != appsv1.RollingUpdateDaemonSetStrategyType {
		t.Fatalf("expected original strategy to be recorded, got %#v", record.DaemonSetStrategy)
	}
	if record.DaemonSetAffinity == nil || len(record.DaemonSetAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) != 1 {
		t.Fatalf("expected original affinity to be recorded, got %#v", record.DaemonSetAffinity)
	}
}

func TestDaemonSetPodReadyOnNodeFindsReadyReplacement(t *testing.T) {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default"},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "agent"}},
		},
	}
	readyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-node-a-new", Namespace: "default", Labels: map[string]string{"app": "agent"}},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	otherNodePod := readyPod.DeepCopy()
	otherNodePod.Name = "agent-node-b"
	otherNodePod.Spec.NodeName = "node-b"
	runner := &Runner{client: fake.NewSimpleClientset(ds, readyPod, otherNodePod)}

	ok, err := runner.daemonSetPodReadyOnNode(testContext(t), "default", "agent", "node-a")
	if err != nil {
		t.Fatalf("daemonSetPodReadyOnNode returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected ready pod on node-a")
	}

	ok, err = runner.daemonSetPodReadyOnNode(testContext(t), "default", "agent", "node-c")
	if err != nil {
		t.Fatalf("daemonSetPodReadyOnNode returned error for missing node: %v", err)
	}
	if ok {
		t.Fatal("unexpected ready pod on node-c")
	}
}
