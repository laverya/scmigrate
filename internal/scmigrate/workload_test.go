package scmigrate

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

func TestQuiesceSharedDeploymentPVCScalesDeploymentToZeroOnce(t *testing.T) {
	pvc := testPVC("default", "data")
	deploy := testDeployment("default", "app", 3, map[string]string{"app": "shared"})
	rs := testReplicaSetForDeployment("default", "app-rs", "app", 3)
	pods := []runtime.Object{
		testPod("default", "app-1", "data", "node-a", controllerRef("ReplicaSet", "app-rs")),
		testPod("default", "app-2", "data", "node-a", controllerRef("ReplicaSet", "app-rs")),
		testPod("default", "app-3", "data", "node-a", controllerRef("ReplicaSet", "app-rs")),
	}
	client := fake.NewSimpleClientset(append([]runtime.Object{deploy, rs}, pods...)...)
	runner := &Runner{client: client, out: io.Discard}

	consumers, err := runner.pvcConsumers(testContext(t), pvc)
	if err != nil {
		t.Fatalf("pvcConsumers returned error: %v", err)
	}
	if len(consumers) != 3 {
		t.Fatalf("expected 3 consumers, got %d", len(consumers))
	}
	for _, consumer := range consumers {
		if consumer.Workload != (WorkloadRef{Kind: "Deployment", Namespace: "default", Name: "app"}) {
			t.Fatalf("unexpected workload ref: %#v", consumer.Workload)
		}
	}

	records, err := runner.quiesceConsumerGroups(testContext(t), pvc, consumers)
	if err != nil {
		t.Fatalf("quiesceConsumerGroups returned error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one deployment record, got %d: %#v", len(records), records)
	}
	record := records[0]
	if record.Kind != "Deployment" || record.Name != "app" || record.OriginalReplicas != 3 {
		t.Fatalf("unexpected record: %#v", record)
	}
	if !reflect.DeepEqual(record.PodNames, []string{"app-1", "app-2", "app-3"}) {
		t.Fatalf("unexpected pod names: %#v", record.PodNames)
	}
	got, err := client.AppsV1().Deployments("default").Get(testContext(t), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("deployment replicas = %v, want 0", got.Spec.Replicas)
	}
}

func TestQuiescePVCSharedByMultipleParentsScalesEveryParent(t *testing.T) {
	pvc := testPVC("default", "shared")
	api := testDeployment("default", "api", 2, map[string]string{"app": "api"})
	apiRS := testReplicaSetForDeployment("default", "api-rs", "api", 2)
	worker := testDeployment("default", "worker", 1, map[string]string{"app": "worker"})
	workerRS := testReplicaSetForDeployment("default", "worker-rs", "worker", 1)
	client := fake.NewSimpleClientset(
		api,
		apiRS,
		worker,
		workerRS,
		testPod("default", "api-1", "shared", "node-a", controllerRef("ReplicaSet", "api-rs")),
		testPod("default", "api-2", "shared", "node-a", controllerRef("ReplicaSet", "api-rs")),
		testPod("default", "worker-1", "shared", "node-a", controllerRef("ReplicaSet", "worker-rs")),
	)
	runner := &Runner{client: client, out: io.Discard}

	consumers, err := runner.pvcConsumers(testContext(t), pvc)
	if err != nil {
		t.Fatalf("pvcConsumers returned error: %v", err)
	}
	records, err := runner.quiesceConsumerGroups(testContext(t), pvc, consumers)
	if err != nil {
		t.Fatalf("quiesceConsumerGroups returned error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected two records, got %d: %#v", len(records), records)
	}
	if records[0].Name != "api" || records[0].OriginalReplicas != 2 {
		t.Fatalf("unexpected api record: %#v", records[0])
	}
	if records[1].Name != "worker" || records[1].OriginalReplicas != 1 {
		t.Fatalf("unexpected worker record: %#v", records[1])
	}
	for _, name := range []string{"api", "worker"} {
		deploy, err := client.AppsV1().Deployments("default").Get(testContext(t), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get deployment/%s: %v", name, err)
		}
		if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != 0 {
			t.Fatalf("deployment/%s replicas = %v, want 0", name, deploy.Spec.Replicas)
		}
	}
}

func TestQuiesceRecordsFromAnnotationAcceptsSingleAndArray(t *testing.T) {
	single := QuiesceRecord{Kind: "Deployment", Namespace: "default", Name: "app", OriginalReplicas: 3}
	rawSingle, err := json.Marshal(single)
	if err != nil {
		t.Fatal(err)
	}
	records, err := quiesceRecordsFromAnnotation(string(rawSingle))
	if err != nil {
		t.Fatalf("single record parse failed: %v", err)
	}
	if len(records) != 1 || records[0].Name != "app" {
		t.Fatalf("unexpected single parse: %#v", records)
	}

	rawArray, err := json.Marshal([]QuiesceRecord{
		single,
		{Kind: "Deployment", Namespace: "default", Name: "worker", OriginalReplicas: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	records, err = quiesceRecordsFromAnnotation(string(rawArray))
	if err != nil {
		t.Fatalf("array record parse failed: %v", err)
	}
	if len(records) != 2 || records[1].Name != "worker" {
		t.Fatalf("unexpected array parse: %#v", records)
	}
}

func TestSyncNodeNameUsesCommonConsumerNode(t *testing.T) {
	pvc := testPVC("default", "data")
	runner := &Runner{client: fake.NewSimpleClientset(
		testPod("default", "app-1", "data", "node-a", controllerRef("ReplicaSet", "app-rs")),
		testPod("default", "app-2", "data", "node-a", controllerRef("ReplicaSet", "app-rs")),
	)}

	node, err := runner.syncNodeName(testContext(t), pvc)
	if err != nil {
		t.Fatalf("syncNodeName returned error: %v", err)
	}
	if node != "node-a" {
		t.Fatalf("node = %q, want node-a", node)
	}

	runner = &Runner{client: fake.NewSimpleClientset(
		testPod("default", "app-1", "data", "node-a", controllerRef("ReplicaSet", "app-rs")),
		testPod("default", "app-2", "data", "node-b", controllerRef("ReplicaSet", "app-rs")),
	)}
	node, err = runner.syncNodeName(testContext(t), pvc)
	if err != nil {
		t.Fatalf("syncNodeName returned error for split nodes: %v", err)
	}
	if node != "" {
		t.Fatalf("node = %q, want empty for split nodes", node)
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

func testPVC(namespace, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
}

func testDeployment(namespace, name string, replicas int32, labels map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
		},
	}
}

func testReplicaSetForDeployment(namespace, name, deployment string, replicas int32) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{controllerRef("Deployment", deployment)},
		},
		Spec: appsv1.ReplicaSetSpec{Replicas: int32Ptr(replicas)},
	}
}

func testPod(namespace, name, claimName, nodeName string, owner metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func controllerRef(kind, name string) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       kind,
		Name:       name,
		Controller: boolPtr(true),
	}
}

func int32Ptr(value int32) *int32 {
	return &value
}

func boolPtr(value bool) *bool {
	return &value
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
