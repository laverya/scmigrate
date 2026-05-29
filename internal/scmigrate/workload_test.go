package scmigrate

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	return context.Background()
}

func TestExcludeNodeFromAffinityCreatesRequiredMatchField(t *testing.T) {
	affinity := excludeNodesFromAffinity(nil, []string{"node-a"})

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

	affinity := excludeNodesFromAffinity(in, []string{"node-b"})
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
	if err := runner.applyQuiesceRecords(testContext(t), records); err != nil {
		t.Fatalf("applyQuiesceRecords returned error: %v", err)
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
	if err := runner.applyQuiesceRecords(testContext(t), records); err != nil {
		t.Fatalf("applyQuiesceRecords returned error: %v", err)
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

func TestQuiesceConsumerGroupsUseClusterWorkloadState(t *testing.T) {
	ctx := testContext(t)
	pvc := testPVC("default", "data")
	standalone := testPod("default", "standalone", "data", "node-a", metav1.OwnerReference{})
	deployPod := testPod("default", "app-0", "data", "node-a", controllerRef("ReplicaSet", "app-rs"))
	rsPod := testPod("default", "worker-0", "data", "node-a", controllerRef("ReplicaSet", "worker-rs"))
	client := fake.NewSimpleClientset(
		standalone,
		testDeployment("default", "app", 2, map[string]string{"app": "app"}),
		testReplicaSetForDeployment("default", "app-rs", "app", 2),
		&appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: "worker-rs", Namespace: "default"},
			Spec:       appsv1.ReplicaSetSpec{Replicas: int32Ptr(1)},
		},
	)
	runner := &Runner{client: client, out: io.Discard}

	ref, err := runner.workloadRefForPod(ctx, standalone)
	if err != nil {
		t.Fatalf("workloadRefForPod returned error: %v", err)
	}
	records, err := runner.quiesceConsumerGroups(ctx, pvc, []PVCConsumer{{Pod: *standalone, Workload: ref}})
	if err != nil {
		t.Fatalf("quiesceConsumerGroups returned error: %v", err)
	}
	record := records[0]
	if record.Kind != "Pod" || record.PodName != "standalone" {
		t.Fatalf("unexpected pod record: %#v", record)
	}
	ref, err = runner.workloadRefForPod(ctx, deployPod)
	if err != nil {
		t.Fatalf("deployment workloadRefForPod returned error: %v", err)
	}
	records, err = runner.quiesceConsumerGroups(ctx, pvc, []PVCConsumer{{Pod: *deployPod, Workload: ref}})
	if err != nil {
		t.Fatalf("deployment quiesceConsumerGroups returned error: %v", err)
	}
	record = records[0]
	if record.Kind != "Deployment" || record.OriginalReplicas != 2 {
		t.Fatalf("unexpected deployment record: %#v", record)
	}
	ref, err = runner.workloadRefForPod(ctx, rsPod)
	if err != nil {
		t.Fatalf("replicaset workloadRefForPod returned error: %v", err)
	}
	records, err = runner.quiesceConsumerGroups(ctx, pvc, []PVCConsumer{{Pod: *rsPod, Workload: ref}})
	if err != nil {
		t.Fatalf("replicaset quiesceConsumerGroups returned error: %v", err)
	}
	record = records[0]
	if record.Kind != "ReplicaSet" || record.OriginalReplicas != 1 {
		t.Fatalf("unexpected replicaset record: %#v", record)
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

	record, err := runner.quiesceDaemonSetObject(testContext(t), WorkloadRef{Kind: WorkloadKindDaemonSet, Namespace: pod.Namespace, Name: ds.Name}, []corev1.Pod{*pod}, ds)
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

func TestQuiesceStandalonePodsDeletesPods(t *testing.T) {
	ctx := testContext(t)
	pods := []corev1.Pod{
		*testPod("default", "standalone-b", "data", "node-b", metav1.OwnerReference{}),
		*testPod("default", "standalone-a", "data", "node-a", metav1.OwnerReference{}),
	}
	client := fake.NewSimpleClientset(&pods[0], &pods[1])
	runner := &Runner{client: client, out: io.Discard}

	record, err := runner.quiesceStandalonePods(ctx, WorkloadRef{Kind: "Pod", Namespace: "default", Name: "standalone-a"}, pods)
	if err != nil {
		t.Fatalf("quiesceStandalonePods returned error: %v", err)
	}
	if record.Kind != "Pod" || record.PodName != "standalone-a" || !reflect.DeepEqual(record.PodNames, []string{"standalone-a", "standalone-b"}) {
		t.Fatalf("unexpected standalone pod record: %#v", record)
	}
	if err := runner.applyQuiesceRecord(ctx, record); err != nil {
		t.Fatalf("applyQuiesceRecord returned error: %v", err)
	}
	for _, name := range []string{"standalone-a", "standalone-b"} {
		if _, err := client.CoreV1().Pods("default").Get(ctx, name, metav1.GetOptions{}); err == nil {
			t.Fatalf("pod/%s still exists after quiesce", name)
		}
	}
}

func TestQuiesceReplicaSetConsumersScalesReplicaSetToZero(t *testing.T) {
	ctx := testContext(t)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-rs", Namespace: "default"},
		Spec:       appsv1.ReplicaSetSpec{Replicas: int32Ptr(2)},
	}
	pods := []corev1.Pod{
		*testPod("default", "worker-1", "data", "node-a", controllerRef("ReplicaSet", "worker-rs")),
		*testPod("default", "worker-2", "data", "node-b", controllerRef("ReplicaSet", "worker-rs")),
	}
	client := fake.NewSimpleClientset(rs)
	runner := &Runner{client: client, out: io.Discard}

	record, err := runner.quiesceReplicaSetConsumers(ctx, WorkloadRef{Kind: "ReplicaSet", Namespace: "default", Name: "worker-rs"}, pods)
	if err != nil {
		t.Fatalf("quiesceReplicaSetConsumers returned error: %v", err)
	}
	if record.Kind != "ReplicaSet" || record.Name != "worker-rs" || record.OriginalReplicas != 2 {
		t.Fatalf("unexpected replicaset record: %#v", record)
	}
	if err := runner.applyQuiesceRecord(ctx, record); err != nil {
		t.Fatalf("applyQuiesceRecord returned error: %v", err)
	}
	got, err := client.AppsV1().ReplicaSets("default").Get(ctx, "worker-rs", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get replicaset: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("replicaset replicas = %v, want 0", got.Spec.Replicas)
	}
}

func TestQuiesceStatefulSetConsumersScalesSharedPVCToZero(t *testing.T) {
	ctx := testContext(t)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(2)},
	}
	pods := []corev1.Pod{
		*testPod("default", "db-0", "data", "node-a", controllerRef("StatefulSet", "db")),
		*testPod("default", "db-1", "data", "node-b", controllerRef("StatefulSet", "db")),
	}
	client := fake.NewSimpleClientset(sts)
	runner := &Runner{client: client, out: io.Discard}

	pvc := testPVC("default", "data")
	record, err := runner.quiesceStatefulSetConsumers(ctx, pvc, WorkloadRef{Kind: "StatefulSet", Namespace: "default", Name: "db"}, pods)
	if err != nil {
		t.Fatalf("quiesceStatefulSetConsumers returned error: %v", err)
	}
	if record.Kind != "StatefulSet" || record.OriginalReplicas != 2 || !reflect.DeepEqual(record.PodNames, []string{"db-0", "db-1"}) {
		t.Fatalf("unexpected statefulset record: %#v", record)
	}
	if err := runner.applyQuiesceRecord(ctx, record); err != nil {
		t.Fatalf("applyQuiesceRecord returned error: %v", err)
	}
	got, err := client.AppsV1().StatefulSets("default").Get(ctx, "db", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("statefulset replicas = %v, want 0", got.Spec.Replicas)
	}
}

func TestQuiesceStatefulSetHighestOrdinalOrphansAndRestoreRecreates(t *testing.T) {
	ctx := testContext(t)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default", UID: "sts-uid"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(3)},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
	pod := testPod("default", "db-2", "data", "node-a", controllerRef("StatefulSet", "db"))
	client := fake.NewSimpleClientset(sts, pod)
	runner := &Runner{client: client, out: io.Discard}

	pvc := boundPVC("default", "data", "source-pv", "old-sc")
	record, err := runner.quiesceStatefulSet(ctx, pvc, pod, sts)
	if err != nil {
		t.Fatalf("quiesceStatefulSet returned error: %v", err)
	}
	if record.Kind != "StatefulSet" || record.OriginalReplicas != 3 || record.StatefulSetConfigMap == "" {
		t.Fatalf("unexpected statefulset record: %#v", record)
	}
	if err := runner.applyQuiesceRecord(ctx, record); err != nil {
		t.Fatalf("applyQuiesceRecord returned error: %v", err)
	}
	if _, err := client.AppsV1().StatefulSets("default").Get(ctx, "db", metav1.GetOptions{}); err == nil {
		t.Fatal("statefulset still exists after orphaning")
	}
	if _, err := client.CoreV1().Pods("default").Get(ctx, "db-2", metav1.GetOptions{}); err == nil {
		t.Fatal("statefulset pod still exists after quiesce")
	}

	if err := runner.restoreOrphanedStatefulSet(ctx, record); err != nil {
		t.Fatalf("restoreOrphanedStatefulSet returned error: %v", err)
	}
	restored, err := client.AppsV1().StatefulSets("default").Get(ctx, "db", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get restored statefulset: %v", err)
	}
	if restored.UID != "" || restored.ResourceVersion != "" || restored.Status.ReadyReplicas != 0 {
		t.Fatalf("restored statefulset kept server-owned fields: %#v", restored)
	}
	if restored.Spec.Replicas == nil || *restored.Spec.Replicas != 3 {
		t.Fatalf("restored statefulset replicas = %v, want 3", restored.Spec.Replicas)
	}
}

func TestQuiesceStoresStatefulSetRecordBeforeDeletingStatefulSet(t *testing.T) {
	ctx := testContext(t)
	source := boundPVC("default", "data", "source-pv", "old-sc")
	source.Annotations[AnnDestinationPVC] = "dest"
	dest := boundPVC("default", "dest", "dest-pv", "new-sc")
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default", UID: "sts-uid"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(3)},
	}
	pod := testPod("default", "db-2", "data", "node-a", controllerRef("StatefulSet", "db"))
	client := fake.NewSimpleClientset(source, dest, sts, pod)
	runner := &Runner{client: client, out: io.Discard}

	if err := runner.quiesce(ctx, source); err != nil {
		t.Fatalf("quiesce returned error: %v", err)
	}
	deleteIndex := actionIndex(client.Actions(), "delete", "statefulsets")
	if deleteIndex < 0 {
		t.Fatalf("statefulset was not deleted; actions: %#v", client.Actions())
	}
	patchesBeforeDelete := 0
	for i, action := range client.Actions() {
		if i >= deleteIndex {
			break
		}
		if action.Matches("patch", "persistentvolumeclaims") {
			patchesBeforeDelete++
		}
	}
	if patchesBeforeDelete < 2 {
		t.Fatalf("source and destination PVCs were not patched before StatefulSet delete; actions: %#v", client.Actions())
	}
	for _, name := range []string{"data", "dest"} {
		pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get pvc/%s: %v", name, err)
		}
		records, err := quiesceRecordsFromAnnotation(pvc.Annotations[AnnQuiesce])
		if err != nil {
			t.Fatalf("decode quiesce records on pvc/%s: %v", name, err)
		}
		if len(records) != 1 || records[0].StatefulSetConfigMap == "" || records[0].StatefulSet != nil {
			t.Fatalf("pvc/%s does not contain compact StatefulSet record reference: %#v", name, records)
		}
	}
	if _, err := client.CoreV1().ConfigMaps("default").Get(ctx, statefulSetRecordName(source, sts), metav1.GetOptions{}); err != nil {
		t.Fatalf("get statefulset restore configmap: %v", err)
	}
}

func TestQuiesceReusesStoredRecordWhenConsumersAlreadyGone(t *testing.T) {
	ctx := testContext(t)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(3)},
	}
	record := QuiesceRecord{
		Kind:             "StatefulSet",
		Name:             "db",
		Namespace:        "default",
		OriginalReplicas: 3,
		PodName:          "db-2",
		PodNames:         []string{"db-2"},
		StatefulSet:      statefulSetForRecreate(sts),
	}
	raw, err := json.Marshal([]QuiesceRecord{record})
	if err != nil {
		t.Fatal(err)
	}
	source := boundPVC("default", "data", "source-pv", "old-sc")
	source.Annotations[AnnDestinationPVC] = "dest"
	source.Annotations[AnnQuiesce] = string(raw)
	dest := boundPVC("default", "dest", "dest-pv", "new-sc")
	runner := &Runner{client: fake.NewSimpleClientset(source, dest), out: io.Discard}

	if err := runner.quiesce(ctx, source); err != nil {
		t.Fatalf("quiesce returned error: %v", err)
	}
	for _, name := range []string{"data", "dest"} {
		pvc, err := runner.client.CoreV1().PersistentVolumeClaims("default").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get pvc/%s: %v", name, err)
		}
		records, err := quiesceRecordsFromAnnotation(pvc.Annotations[AnnQuiesce])
		if err != nil {
			t.Fatalf("decode quiesce records on pvc/%s: %v", name, err)
		}
		if len(records) != 1 || records[0].Kind != "StatefulSet" || records[0].StatefulSet == nil {
			t.Fatalf("quiesce overwrote stored StatefulSet record on pvc/%s: %#v", name, records)
		}
	}
}

func TestQuiesceStatefulSetRejectsOrdinalOutsideReplicaRange(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(3)},
	}
	pvc := boundPVC("default", "data", "source-pv", "old-sc")
	pod := testPod("default", "db-3", "data", "node-a", controllerRef("StatefulSet", "db"))
	runner := &Runner{client: fake.NewSimpleClientset(), out: io.Discard}

	_, err := runner.quiesceStatefulSet(testContext(t), pvc, pod, sts)
	if err == nil || !strings.Contains(err.Error(), "outside current replica count") {
		t.Fatalf("quiesceStatefulSet error = %v, want outside replica count error", err)
	}
}

func TestQuiesceDaemonSetConsumersPatchesSchedulingAndDeletesPods(t *testing.T) {
	ctx := testContext(t)
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default"},
		Spec: appsv1.DaemonSetSpec{
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.RollingUpdateDaemonSetStrategyType},
			Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{}},
		},
	}
	pods := []corev1.Pod{
		*testPod("default", "agent-a", "data", "node-a", controllerRef("DaemonSet", "agent")),
		*testPod("default", "agent-b", "data", "node-b", controllerRef("DaemonSet", "agent")),
	}
	client := fake.NewSimpleClientset(ds, &pods[0], &pods[1])
	runner := &Runner{client: client, out: io.Discard}

	record, err := runner.quiesceDaemonSetConsumers(ctx, WorkloadRef{Kind: "DaemonSet", Namespace: "default", Name: "agent"}, pods)
	if err != nil {
		t.Fatalf("quiesceDaemonSetConsumers returned error: %v", err)
	}
	if record.Kind != "DaemonSet" || !reflect.DeepEqual(record.NodeNames, []string{"node-a", "node-b"}) {
		t.Fatalf("unexpected daemonset record: %#v", record)
	}
	if err := runner.applyQuiesceRecord(ctx, record); err != nil {
		t.Fatalf("applyQuiesceRecord returned error: %v", err)
	}
	patched, err := client.AppsV1().DaemonSets("default").Get(ctx, "agent", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get daemonset: %v", err)
	}
	if patched.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType {
		t.Fatalf("daemonset strategy = %s, want OnDelete", patched.Spec.UpdateStrategy.Type)
	}
	fields := patched.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields
	if len(fields) != 1 || fields[0].Operator != corev1.NodeSelectorOpNotIn || !reflect.DeepEqual(fields[0].Values, []string{"node-a", "node-b"}) {
		t.Fatalf("unexpected daemonset node exclusion: %#v", fields)
	}
	for _, name := range []string{"agent-a", "agent-b"} {
		if _, err := client.CoreV1().Pods("default").Get(ctx, name, metav1.GetOptions{}); err == nil {
			t.Fatalf("pod/%s still exists after daemonset quiesce", name)
		}
	}
}

func TestRestoreQuiesceRecordRestoresControllerState(t *testing.T) {
	ctx := testContext(t)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-rs", Namespace: "default"},
		Spec:       appsv1.ReplicaSetSpec{Replicas: int32Ptr(0)},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: int32Ptr(0)},
	}
	originalAffinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      "storage",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"fast"},
				}}}},
			},
		},
	}
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default"},
		Spec: appsv1.DaemonSetSpec{
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
			Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{Affinity: excludeNodesFromAffinity(originalAffinity, []string{"node-a"})}},
		},
	}
	client := fake.NewSimpleClientset(rs, sts, ds)
	runner := &Runner{client: client, out: io.Discard}

	records := []QuiesceRecord{
		{Kind: "ReplicaSet", Namespace: "default", Name: "worker-rs", OriginalReplicas: 2},
		{Kind: "StatefulSet", Namespace: "default", Name: "db", OriginalReplicas: 1},
		{
			Kind:              "DaemonSet",
			Namespace:         "default",
			Name:              "agent",
			DaemonSetStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.RollingUpdateDaemonSetStrategyType},
			DaemonSetAffinity: originalAffinity,
		},
	}
	for _, record := range records {
		if err := runner.restoreQuiesceRecord(ctx, record); err != nil {
			t.Fatalf("restoreQuiesceRecord(%s) returned error: %v", record.Kind, err)
		}
	}
	gotRS, err := client.AppsV1().ReplicaSets("default").Get(ctx, "worker-rs", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get replicaset: %v", err)
	}
	if gotRS.Spec.Replicas == nil || *gotRS.Spec.Replicas != 2 {
		t.Fatalf("replicaset replicas = %v, want 2", gotRS.Spec.Replicas)
	}
	gotSTS, err := client.AppsV1().StatefulSets("default").Get(ctx, "db", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	if gotSTS.Spec.Replicas == nil || *gotSTS.Spec.Replicas != 1 {
		t.Fatalf("statefulset replicas = %v, want 1", gotSTS.Spec.Replicas)
	}
	gotDS, err := client.AppsV1().DaemonSets("default").Get(ctx, "agent", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get daemonset: %v", err)
	}
	if gotDS.Spec.UpdateStrategy.Type != appsv1.RollingUpdateDaemonSetStrategyType || !reflect.DeepEqual(gotDS.Spec.Template.Spec.Affinity, originalAffinity) {
		t.Fatalf("daemonset was not restored: %#v", gotDS.Spec)
	}
}

func TestWaitWorkloadRestoredRecognizesReadyControllers(t *testing.T) {
	ctx := testContext(t)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-rs", Namespace: "default"},
		Status:     appsv1.ReplicaSetStatus{ReadyReplicas: 2},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "default"},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "agent"}},
		},
	}
	readyDaemonPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "agent-node-a", Namespace: "default", Labels: map[string]string{"app": "agent"}},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	runner := &Runner{client: fake.NewSimpleClientset(rs, sts, ds, readyDaemonPod), out: io.Discard}

	records := []QuiesceRecord{
		{Kind: "None"},
		{Kind: "Pod"},
		{Kind: "ReplicaSet", Namespace: "default", Name: "worker-rs", OriginalReplicas: 2},
		{Kind: "StatefulSet", Namespace: "default", Name: "db", OriginalReplicas: 1},
		{Kind: "DaemonSet", Namespace: "default", Name: "agent", NodeNames: []string{"node-a"}},
	}
	for _, record := range records {
		if err := runner.waitWorkloadRestored(ctx, record); err != nil {
			t.Fatalf("waitWorkloadRestored(%s) returned error: %v", record.Kind, err)
		}
	}
}

func TestStoreQuiesceRecordPatchesSourcePVC(t *testing.T) {
	ctx := testContext(t)
	pvc := boundPVC("default", "data", "source-pv", "old-sc")
	runner := &Runner{client: fake.NewSimpleClientset(pvc), out: io.Discard}

	if err := runner.storeQuiesceRecords(ctx, pvc, []QuiesceRecord{{Kind: "None", Namespace: "default"}}); err != nil {
		t.Fatalf("storeQuiesceRecords returned error: %v", err)
	}
	updated, err := runner.client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	records, err := quiesceRecordsFromAnnotation(updated.Annotations[AnnQuiesce])
	if err != nil {
		t.Fatalf("decode quiesce annotation: %v", err)
	}
	if len(records) != 1 || records[0].Kind != "None" {
		t.Fatalf("unexpected quiesce annotation: %#v", records)
	}
}

func TestQuiesceDeploymentWithZeroReplicasReturnsError(t *testing.T) {
	deploy := testDeployment("default", "app", 0, map[string]string{"app": "db"})
	runner := &Runner{client: fake.NewSimpleClientset(deploy), out: io.Discard}

	_, err := runner.quiesceDeploymentConsumers(testContext(t), testPVC("default", "data"), WorkloadRef{Kind: "Deployment", Namespace: "default", Name: "app"}, []corev1.Pod{
		*testPod("default", "app-0", "data", "node-a", controllerRef("ReplicaSet", "app-rs")),
	})
	if err == nil || !strings.Contains(err.Error(), "no replicas") {
		t.Fatalf("quiesceDeploymentConsumers error = %v, want no replicas", err)
	}
}

func TestWorkloadRefForPodRejectsUnsupportedController(t *testing.T) {
	pod := testPod("default", "job-pod", "data", "node-a", controllerRef("Job", "batch"))
	runner := &Runner{client: fake.NewSimpleClientset(), out: io.Discard}

	_, err := runner.workloadRefForPod(testContext(t), pod)
	if err == nil || !strings.Contains(err.Error(), "unsupported pod controller") {
		t.Fatalf("workloadRefForPod error = %v, want unsupported controller", err)
	}
}

func TestRestoreQuiesceRecordRejectsUnsupportedKind(t *testing.T) {
	runner := &Runner{client: fake.NewSimpleClientset(), out: io.Discard}

	err := runner.restoreQuiesceRecord(testContext(t), QuiesceRecord{Kind: "Job", Namespace: "default", Name: "batch"})
	if err == nil || !strings.Contains(err.Error(), "unsupported quiesce record") {
		t.Fatalf("restoreQuiesceRecord error = %v, want unsupported kind", err)
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

func actionIndex(actions []ktesting.Action, verb, resource string) int {
	for i, action := range actions {
		if action.Matches(verb, resource) {
			return i
		}
	}
	return -1
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
