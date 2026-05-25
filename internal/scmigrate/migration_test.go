package scmigrate

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestPrepareCreatesDestinationAndStoresDurableAnnotations(t *testing.T) {
	ctx := testContext(t)
	source := boundPVC("default", "data", "source-pv", "old-sc")
	source.Labels["app"] = "db"
	source.Annotations["keep"] = "yes"
	source.Annotations["pv.kubernetes.io/bind-completed"] = "yes"
	client := fake.NewSimpleClientset(source)
	runner := &Runner{
		opts:   Options{TargetStorageClass: "new-sc"},
		client: client,
		out:    io.Discard,
	}

	if err := runner.prepare(ctx, source); err != nil {
		t.Fatalf("prepare returned error: %v", err)
	}

	updatedSource, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get source pvc: %v", err)
	}
	destName := tempPVCName(source)
	wantSourceAnnotations := map[string]string{
		AnnState:              StatePrepared,
		AnnDestinationPVC:     destName,
		AnnSourcePV:           "source-pv",
		AnnTargetStorageClass: "new-sc",
	}
	for key, want := range wantSourceAnnotations {
		if got := updatedSource.Annotations[key]; got != want {
			t.Fatalf("source annotation %s = %q, want %q", key, got, want)
		}
	}
	if updatedSource.Annotations[AnnOriginalPVC] == "" {
		t.Fatal("source is missing original PVC snapshot annotation")
	}
	stored, err := storedPVC(updatedSource.Annotations[AnnOriginalPVC])
	if err != nil {
		t.Fatalf("decode original PVC snapshot: %v", err)
	}
	if stored.Name != "data" || stored.Namespace != "default" || stored.Labels["app"] != "db" {
		t.Fatalf("unexpected stored PVC identity: %#v", stored)
	}
	if stored.Annotations["keep"] != "yes" {
		t.Fatalf("stored annotations lost user annotation: %#v", stored.Annotations)
	}
	if _, ok := stored.Annotations["pv.kubernetes.io/bind-completed"]; ok {
		t.Fatalf("stored annotations kept Kubernetes binding annotation: %#v", stored.Annotations)
	}

	dest, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, destName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get destination pvc: %v", err)
	}
	if dest.Labels[LabelManagedBy] != "scmigrate" || dest.Labels[LabelRole] != "destination" || dest.Labels[LabelSourceUID] != string(source.UID) {
		t.Fatalf("unexpected destination labels: %#v", dest.Labels)
	}
	if storageClass(dest) != "new-sc" {
		t.Fatalf("destination storage class = %q, want new-sc", storageClass(dest))
	}
	if dest.Annotations[AnnSourcePVC] != "data" || dest.Annotations[AnnSourcePV] != "source-pv" || dest.Annotations[AnnOriginalPVC] == "" {
		t.Fatalf("unexpected destination annotations: %#v", dest.Annotations)
	}
	if got := dest.Spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("10Gi")) != 0 {
		t.Fatalf("destination storage request = %s, want 10Gi", got.String())
	}
}

func TestDiscoverUsesClusterStateAndSortsResumablePVCs(t *testing.T) {
	ctx := testContext(t)
	data0 := boundPVC("default", "data-0", "source-pv-0", "old-sc")
	data0.Labels["app"] = "db"
	data0.Annotations["approved"] = "true"
	data0.Annotations[AnnState] = StateQuiesced
	data0.Annotations[AnnDestinationPVC] = "dest-data-0"
	data2 := boundPVC("default", "data-2", "source-pv-2", "old-sc")
	data2.Labels["app"] = "db"
	data2.Annotations["approved"] = "true"
	skippedTarget := boundPVC("default", "already-new", "already-pv", "new-sc")
	skippedTarget.Labels["app"] = "db"
	skippedTarget.Annotations["approved"] = "true"
	managed := boundPVC("default", "managed", "managed-pv", "old-sc")
	managed.Labels["app"] = "db"
	managed.Labels[LabelManagedBy] = "scmigrate"
	managed.Annotations["approved"] = "true"

	dest0 := boundPVC("default", "dest-data-0", "dest-pv-0", "new-sc")
	dest0.Labels[LabelManagedBy] = "scmigrate"
	dest0.Labels[LabelRole] = "destination"
	client := fake.NewSimpleClientset(
		data0,
		data2,
		skippedTarget,
		managed,
		dest0,
		pv("source-pv-0", "default", "data-0", corev1.PersistentVolumeReclaimDelete),
		pv("source-pv-2", "default", "data-2", corev1.PersistentVolumeReclaimDelete),
		pv("dest-pv-0", "default", "dest-data-0", corev1.PersistentVolumeReclaimDelete),
		testDeployment("default", "app", 1, map[string]string{"app": "db"}),
		testReplicaSetForDeployment("default", "app-rs", "app", 1),
		testPod("default", "app-0", "data-0", "node-a", controllerRef("ReplicaSet", "app-rs")),
	)
	runner := &Runner{
		opts: Options{
			Namespace:          "default",
			Selector:           "app=db",
			AnnotationFilters:  []string{"approved=true"},
			SourceStorageClass: "old-sc",
			TargetStorageClass: "new-sc",
		},
		client: client,
		out:    io.Discard,
	}

	migrations, err := runner.discover(ctx)
	if err != nil {
		t.Fatalf("discover returned error: %v", err)
	}
	if got, want := migrationNames(migrations), []string{"data-2", "data-0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("migration order = %#v, want %#v", got, want)
	}
	if migrations[0].State != "new" {
		t.Fatalf("data-2 state = %q, want new", migrations[0].State)
	}
	if migrations[1].State != StateQuiesced {
		t.Fatalf("data-0 state = %q, want %s", migrations[1].State, StateQuiesced)
	}
	if migrations[1].SourcePV.Name != "source-pv-0" || migrations[1].Destination.Name != "dest-data-0" || migrations[1].DestPV.Name != "dest-pv-0" {
		t.Fatalf("discover did not hydrate PV/destination state: %#v", migrations[1])
	}
	if len(migrations[1].Consumers) != 1 || migrations[1].Consumers[0].Workload.Name != "app" {
		t.Fatalf("unexpected consumers: %#v", migrations[1].Consumers)
	}
}

func TestMigrateResumesFromClusterStateInsteadOfMigrationStruct(t *testing.T) {
	ctx := testContext(t)
	source := boundPVC("default", "data", "source-pv", "old-sc")
	destName := tempPVCName(source)
	source.Annotations[AnnState] = StateQuiesced
	source.Annotations[AnnDestinationPVC] = destName
	snapshot, err := pvcSnapshot(source)
	if err != nil {
		t.Fatal(err)
	}
	dest := boundPVC("default", destName, "dest-pv", "new-sc")
	dest.Labels[LabelManagedBy] = "scmigrate"
	dest.Labels[LabelRole] = "destination"
	dest.Annotations[AnnSourcePVC] = source.Name
	dest.Annotations[AnnOriginalPVC] = snapshot
	dest.Annotations[AnnState] = StateFinalSynced
	client := fake.NewSimpleClientset(
		source,
		dest,
		pv("source-pv", "default", "data", corev1.PersistentVolumeReclaimDelete),
		pv("dest-pv", "default", "dest", corev1.PersistentVolumeReclaimDelete),
	)
	var createdPods []corev1.Pod
	succeedCreatedPods(client, &createdPods)
	bindCreatedPVCs(client)
	runner := &Runner{
		opts: Options{
			TargetStorageClass: "new-sc",
			RunnerImage:        "sync-image",
			RsyncArgs:          "-a",
		},
		client: client,
		out:    io.Discard,
	}
	staleMigration := &Migration{Source: source.DeepCopy(), State: "new"}

	if err := runner.migrate(ctx, staleMigration); err != nil {
		t.Fatalf("migrate returned error: %v", err)
	}

	if len(createdPods) != 1 {
		t.Fatalf("created sync pods = %d, want only final sync: %#v", len(createdPods), createdPods)
	}
	if !strings.Contains(createdPods[0].Name, "final") {
		t.Fatalf("created pod %q, want final sync pod", createdPods[0].Name)
	}
	final, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get final pvc: %v", err)
	}
	if final.Annotations[AnnState] != StateRestored {
		t.Fatalf("final PVC state = %q, want %s", final.Annotations[AnnState], StateRestored)
	}
	if final.Spec.VolumeName != "dest-pv" || storageClass(final) != "new-sc" {
		t.Fatalf("unexpected final PVC binding: volume=%q storageClass=%q", final.Spec.VolumeName, storageClass(final))
	}
	destPV, err := client.CoreV1().PersistentVolumes().Get(ctx, "dest-pv", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get dest pv: %v", err)
	}
	if destPV.Spec.ClaimRef != nil {
		t.Fatalf("dest PV claimRef was not cleared: %#v", destPV.Spec.ClaimRef)
	}
	if destPV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("dest PV reclaim policy = %s, want Retain", destPV.Spec.PersistentVolumeReclaimPolicy)
	}
}

func TestDiscoverAndMigrateResumeAfterSourcePVCWasDeleted(t *testing.T) {
	ctx := testContext(t)
	original := boundPVC("default", "data", "source-pv", "old-sc")
	original.Labels["app"] = "db"
	original.Annotations["approved"] = "true"
	destName := tempPVCName(original)
	snapshot, err := pvcSnapshot(original)
	if err != nil {
		t.Fatal(err)
	}
	dest := boundPVC("default", destName, "dest-pv", "new-sc")
	dest.Labels[LabelManagedBy] = "scmigrate"
	dest.Labels[LabelRole] = "destination"
	dest.Annotations[AnnState] = StateFinalSynced
	dest.Annotations[AnnSourcePVC] = original.Name
	dest.Annotations[AnnSourceUID] = string(original.UID)
	dest.Annotations[AnnSourcePV] = original.Spec.VolumeName
	dest.Annotations[AnnOriginalPVC] = snapshot
	dest.Annotations[AnnQuiesce] = `[{"kind":"None","namespace":"default"}]`
	client := fake.NewSimpleClientset(dest, pv("dest-pv", "default", destName, corev1.PersistentVolumeReclaimRetain))
	bindCreatedPVCs(client)
	runner := &Runner{
		opts: Options{
			Namespace:          "default",
			Selector:           "app=db",
			AnnotationFilters:  []string{"approved=true"},
			SourceStorageClass: "old-sc",
			TargetStorageClass: "new-sc",
		},
		client: client,
		out:    io.Discard,
	}

	migrations, err := runner.discover(ctx)
	if err != nil {
		t.Fatalf("discover returned error: %v", err)
	}
	if len(migrations) != 1 {
		t.Fatalf("migrations = %d, want 1: %#v", len(migrations), migrations)
	}
	if migrations[0].Source.Name != "data" || migrations[0].Destination.Name != destName || migrations[0].State != StateFinalSynced {
		t.Fatalf("unexpected resumable migration: %#v", migrations[0])
	}
	if err := runner.migrate(ctx, migrations[0]); err != nil {
		t.Fatalf("migrate returned error: %v", err)
	}
	final, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get final pvc: %v", err)
	}
	if final.Annotations[AnnState] != StateRestored || final.Spec.VolumeName != "dest-pv" {
		t.Fatalf("unexpected final pvc after source-deleted resume: %#v", final)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, destName, metav1.GetOptions{}); err == nil {
		t.Fatal("temporary destination PVC still exists after source-deleted resume")
	}
}

func TestMigrateRestoresCutoverPVCWithoutPreparingAgain(t *testing.T) {
	ctx := testContext(t)
	final := boundPVC("default", "data", "dest-pv", "new-sc")
	final.Annotations[AnnState] = StateCutover
	final.Annotations[AnnDestinationPVC] = "original-temp"
	final.Annotations[AnnQuiesce] = `[{"kind":"None","namespace":"default"}]`
	runner := &Runner{opts: Options{TargetStorageClass: "new-sc"}, client: fake.NewSimpleClientset(final), out: io.Discard}

	if err := runner.migrate(ctx, &Migration{Source: final.DeepCopy(), State: StateCutover}); err != nil {
		t.Fatalf("migrate returned error: %v", err)
	}

	restored, err := runner.client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if restored.Annotations[AnnState] != StateRestored {
		t.Fatalf("state = %q, want restored", restored.Annotations[AnnState])
	}
	if restored.Annotations[AnnDestinationPVC] != "original-temp" {
		t.Fatalf("prepare overwrote destination annotation: %#v", restored.Annotations)
	}
}

func TestDryRunFreshPVCPrintsFullPlanWithoutDestinationPVC(t *testing.T) {
	ctx := testContext(t)
	source := boundPVC("default", "data", "source-pv", "old-sc")
	client := fake.NewSimpleClientset(
		source,
		pv("source-pv", "default", "data", corev1.PersistentVolumeReclaimDelete),
	)
	var out bytes.Buffer
	runner := &Runner{
		opts: Options{
			TargetStorageClass: "new-sc",
			RunnerImage:        "sync-image",
			RsyncArgs:          "-a",
			DryRun:             true,
		},
		client: client,
		out:    &out,
	}

	if err := runner.migrate(ctx, &Migration{Source: source}); err != nil {
		t.Fatalf("dry-run migrate returned error: %v\n%s", err, out.String())
	}

	got := out.String()
	for _, want := range []string{
		"dry-run: create destination pvc/",
		"dry-run: run initial sync pod/",
		"dry-run: store 1 quiesce record(s)",
		"dry-run: run final sync pod/",
		"dry-run: delete pvc/data",
		"dry-run: restore workload(s)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("dry-run output missing %q:\n%s", want, got)
		}
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, tempPVCName(source), metav1.GetOptions{}); err == nil {
		t.Fatal("dry-run created a destination PVC")
	}
}

func TestPrepareRejectsUnsupportedPVCModes(t *testing.T) {
	ctx := testContext(t)
	blockMode := corev1.PersistentVolumeBlock
	block := boundPVC("default", "block", "block-pv", "old-sc")
	block.Spec.VolumeMode = &blockMode
	rwop := boundPVC("default", "rwop", "rwop-pv", "old-sc")
	rwop.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
	runner := &Runner{opts: Options{TargetStorageClass: "new-sc"}, client: fake.NewSimpleClientset(block, rwop), out: io.Discard}

	if err := runner.prepare(ctx, block); err == nil || !strings.Contains(err.Error(), "block volumeMode") {
		t.Fatalf("prepare block volume error = %v, want block rejection", err)
	}
	if err := runner.prepare(ctx, rwop); err == nil || !strings.Contains(err.Error(), "ReadWriteOncePod") {
		t.Fatalf("prepare rwop error = %v, want ReadWriteOncePod rejection", err)
	}
}

func TestDiscoverRejectsTargetStorageClassDrift(t *testing.T) {
	ctx := testContext(t)
	source := boundPVC("default", "data", "source-pv", "old-sc")
	source.Annotations[AnnState] = StatePrepared
	source.Annotations[AnnTargetStorageClass] = "previous-sc"
	runner := &Runner{
		opts:   Options{Namespace: "default", TargetStorageClass: "new-sc"},
		client: fake.NewSimpleClientset(source, pv("source-pv", "default", "data", corev1.PersistentVolumeReclaimDelete)),
		out:    io.Discard,
	}

	_, err := runner.discover(ctx)
	if err == nil || !strings.Contains(err.Error(), "previous-sc") {
		t.Fatalf("discover error = %v, want target storage class drift", err)
	}
}

func TestResumeCutoverFromPVRecordAfterTemporaryPVCWasDeleted(t *testing.T) {
	ctx := testContext(t)
	original := boundPVC("default", "data", "source-pv", "old-sc")
	snapshot, err := pvcSnapshot(original)
	if err != nil {
		t.Fatal(err)
	}
	destPV := pv("dest-pv", "default", "dest", corev1.PersistentVolumeReclaimRetain)
	destPV.Annotations[AnnState] = StateFinalSynced
	destPV.Annotations[AnnSourcePVC] = "data"
	destPV.Annotations[AnnSourceNamespace] = "default"
	destPV.Annotations[AnnSourceUID] = string(original.UID)
	destPV.Annotations[AnnSourcePV] = "source-pv"
	destPV.Annotations[AnnDestinationPVC] = "dest"
	destPV.Annotations[AnnDestinationPV] = "dest-pv"
	destPV.Annotations[AnnTargetStorageClass] = "new-sc"
	destPV.Annotations[AnnOriginalPVC] = snapshot
	destPV.Annotations[AnnQuiesce] = `[{"kind":"None","namespace":"default"}]`
	client := fake.NewSimpleClientset(destPV)
	bindCreatedPVCs(client)
	runner := &Runner{opts: Options{TargetStorageClass: "new-sc"}, client: client, out: io.Discard}

	if err := runner.resumeCutoverWithoutSource(ctx, "default", "data"); err != nil {
		t.Fatalf("resumeCutoverWithoutSource returned error: %v", err)
	}

	final, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get final pvc: %v", err)
	}
	if final.Spec.VolumeName != "dest-pv" || final.Annotations[AnnQuiesce] == "" {
		t.Fatalf("unexpected final pvc from PV record: %#v", final)
	}
	updatedPV, err := client.CoreV1().PersistentVolumes().Get(ctx, "dest-pv", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get dest pv: %v", err)
	}
	if updatedPV.Spec.ClaimRef != nil {
		t.Fatalf("dest PV claimRef was not cleared: %#v", updatedPV.Spec.ClaimRef)
	}
}

func TestMigrateRunsAllPhasesFromFreshPVCUsingClusterState(t *testing.T) {
	ctx := testContext(t)
	source := boundPVC("default", "data", "source-pv", "old-sc")
	destName := tempPVCName(source)
	client := fake.NewSimpleClientset(
		source,
		pv("source-pv", "default", "data", corev1.PersistentVolumeReclaimDelete),
		pv("dest-pv", "default", destName, corev1.PersistentVolumeReclaimDelete),
	)
	var createdPods []corev1.Pod
	succeedCreatedPods(client, &createdPods)
	bindCreatedPVCsToPV(client, "dest-pv")
	runner := &Runner{
		opts: Options{
			TargetStorageClass:   "new-sc",
			RunnerImage:          "sync-image",
			RsyncArgs:            "-a",
			RestoreReclaimPolicy: true,
		},
		client: client,
		out:    io.Discard,
	}

	if err := runner.migrate(ctx, &Migration{Source: source}); err != nil {
		t.Fatalf("migrate returned error: %v", err)
	}

	if got, want := syncPodPhases(createdPods), []string{"initial", "final"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sync phases = %#v, want %#v", got, want)
	}
	final, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get final pvc: %v", err)
	}
	if final.Annotations[AnnState] != StateRestored || final.Annotations[AnnQuiesce] == "" {
		t.Fatalf("unexpected final annotations: %#v", final.Annotations)
	}
	if final.Spec.VolumeName != "dest-pv" || storageClass(final) != "new-sc" {
		t.Fatalf("unexpected final PVC binding: volume=%q storageClass=%q", final.Spec.VolumeName, storageClass(final))
	}
	sourcePV, err := client.CoreV1().PersistentVolumes().Get(ctx, "source-pv", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get source pv: %v", err)
	}
	if sourcePV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("source PV reclaim policy = %s, want Retain", sourcePV.Spec.PersistentVolumeReclaimPolicy)
	}
	destPV, err := client.CoreV1().PersistentVolumes().Get(ctx, "dest-pv", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get dest pv: %v", err)
	}
	if destPV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Fatalf("dest PV reclaim policy = %s, want restored Delete", destPV.Spec.PersistentVolumeReclaimPolicy)
	}
	if destPV.Spec.ClaimRef != nil {
		t.Fatalf("dest PV claimRef was not cleared: %#v", destPV.Spec.ClaimRef)
	}
}

func TestRunSyncCreatesPinnedInitialSyncPodAndCleansItUp(t *testing.T) {
	ctx := testContext(t)
	source := boundPVC("default", "data", "source-pv", "old-sc")
	source.Annotations[AnnDestinationPVC] = "dest"
	dest := boundPVC("default", "dest", "dest-pv", "new-sc")
	client := fake.NewSimpleClientset(
		source,
		dest,
		testPod("default", "consumer", "data", "node-a", metav1.OwnerReference{}),
	)
	var createdPods []corev1.Pod
	succeedCreatedPods(client, &createdPods)
	runner := &Runner{
		opts:   Options{TargetStorageClass: "new-sc", RunnerImage: "sync-image", RsyncArgs: "-a --delete"},
		client: client,
		out:    io.Discard,
	}

	if err := runner.runSync(ctx, source, "initial"); err != nil {
		t.Fatalf("runSync returned error: %v", err)
	}

	if len(createdPods) != 1 {
		t.Fatalf("created pods = %d, want 1", len(createdPods))
	}
	pod := createdPods[0]
	if pod.Spec.Affinity == nil {
		t.Fatal("initial sync pod was not pinned to the consumer node")
	}
	values := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].Values
	if !reflect.DeepEqual(values, []string{"node-a"}) {
		t.Fatalf("sync pod node affinity values = %#v, want node-a", values)
	}
	if pod.Spec.Containers[0].Image != "sync-image" || !reflect.DeepEqual(pod.Spec.Containers[0].Args, []string{"rsync -a --delete /source/ /destination/"}) {
		t.Fatalf("unexpected sync container: %#v", pod.Spec.Containers[0])
	}
	if _, err := client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{}); err == nil {
		t.Fatalf("sync pod %s still exists after cleanup", pod.Name)
	}
}

func TestQuiesceStoresRecordsOnSourceAndDestinationPVCs(t *testing.T) {
	ctx := testContext(t)
	source := boundPVC("default", "data", "source-pv", "old-sc")
	source.Annotations[AnnDestinationPVC] = "dest"
	dest := boundPVC("default", "dest", "dest-pv", "new-sc")
	client := fake.NewSimpleClientset(source, dest)
	runner := &Runner{client: client, out: io.Discard}

	if err := runner.quiesce(ctx, source); err != nil {
		t.Fatalf("quiesce returned error: %v", err)
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
		if len(records) != 1 || records[0].Kind != "None" || records[0].Namespace != "default" {
			t.Fatalf("unexpected quiesce records on pvc/%s: %#v", name, records)
		}
	}
}

func TestResumeCutoverWithoutSourceCreatesFinalPVCFromDestination(t *testing.T) {
	ctx := testContext(t)
	original := boundPVC("default", "data", "source-pv", "old-sc")
	snapshot, err := pvcSnapshot(original)
	if err != nil {
		t.Fatal(err)
	}
	dest := boundPVC("default", "dest", "dest-pv", "new-sc")
	dest.Labels[LabelManagedBy] = "scmigrate"
	dest.Labels[LabelRole] = "destination"
	dest.Annotations[AnnSourcePVC] = "data"
	dest.Annotations[AnnOriginalPVC] = snapshot
	dest.Annotations[AnnQuiesce] = `[{"kind":"None","namespace":"default"}]`
	client := fake.NewSimpleClientset(dest, pv("dest-pv", "default", "dest", corev1.PersistentVolumeReclaimRetain))
	bindCreatedPVCs(client)
	runner := &Runner{opts: Options{TargetStorageClass: "new-sc"}, client: client, out: io.Discard}

	if err := runner.resumeCutoverWithoutSource(ctx, "default", "data"); err != nil {
		t.Fatalf("resumeCutoverWithoutSource returned error: %v", err)
	}

	final, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get final pvc: %v", err)
	}
	if final.Annotations[AnnState] != StateCutover || final.Annotations[AnnQuiesce] == "" || final.Spec.VolumeName != "dest-pv" {
		t.Fatalf("unexpected final pvc: %#v", final)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "dest", metav1.GetOptions{}); err == nil {
		t.Fatal("temporary destination PVC still exists")
	}
	destPV, err := client.CoreV1().PersistentVolumes().Get(ctx, "dest-pv", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get dest pv: %v", err)
	}
	if destPV.Spec.ClaimRef != nil {
		t.Fatalf("dest PV claimRef was not cleared: %#v", destPV.Spec.ClaimRef)
	}
}

func TestRestoreWorkloadReadsQuiesceAnnotationFromCluster(t *testing.T) {
	ctx := testContext(t)
	record := QuiesceRecord{Kind: "Deployment", Namespace: "default", Name: "app", OriginalReplicas: 3}
	recordRaw, err := json.Marshal([]QuiesceRecord{record})
	if err != nil {
		t.Fatal(err)
	}
	pvc := boundPVC("default", "data", "dest-pv", "new-sc")
	pvc.Annotations[AnnQuiesce] = string(recordRaw)
	deploy := testDeployment("default", "app", 0, map[string]string{"app": "db"})
	deploy.Status.ReadyReplicas = 3
	client := fake.NewSimpleClientset(pvc, deploy)
	runner := &Runner{client: client, out: io.Discard}

	if err := runner.restoreWorkload(ctx, pvc.DeepCopy()); err != nil {
		t.Fatalf("restoreWorkload returned error: %v", err)
	}

	restored, err := client.AppsV1().Deployments("default").Get(ctx, "app", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if restored.Spec.Replicas == nil || *restored.Spec.Replicas != 3 {
		t.Fatalf("deployment replicas = %v, want 3", restored.Spec.Replicas)
	}
	finalPVC, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if finalPVC.Annotations[AnnState] != StateRestored {
		t.Fatalf("PVC state = %q, want %s", finalPVC.Annotations[AnnState], StateRestored)
	}
}

func TestRestoreWorkloadRejectsInvalidQuiesceAnnotation(t *testing.T) {
	ctx := testContext(t)
	pvc := boundPVC("default", "data", "dest-pv", "new-sc")
	pvc.Annotations[AnnQuiesce] = "{not json"
	runner := &Runner{client: fake.NewSimpleClientset(pvc), out: io.Discard}

	if err := runner.restoreWorkload(ctx, pvc); err == nil {
		t.Fatal("restoreWorkload returned nil for malformed quiesce annotation")
	}
}

func TestResumeCutoverWithoutSourceErrorsWhenDestinationIsUnbound(t *testing.T) {
	ctx := testContext(t)
	original := boundPVC("default", "data", "source-pv", "old-sc")
	snapshot, err := pvcSnapshot(original)
	if err != nil {
		t.Fatal(err)
	}
	dest := boundPVC("default", "dest", "", "new-sc")
	dest.Status.Phase = corev1.ClaimPending
	dest.Labels[LabelManagedBy] = "scmigrate"
	dest.Labels[LabelRole] = "destination"
	dest.Annotations[AnnSourcePVC] = "data"
	dest.Annotations[AnnOriginalPVC] = snapshot
	runner := &Runner{opts: Options{TargetStorageClass: "new-sc"}, client: fake.NewSimpleClientset(dest), out: io.Discard}

	err = runner.resumeCutoverWithoutSource(ctx, "default", "data")
	if err == nil || !strings.Contains(err.Error(), "is not bound") {
		t.Fatalf("resumeCutoverWithoutSource error = %v, want destination not bound", err)
	}
}

func boundPVC(namespace, name, volumeName, storageClassName string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			UID:         types.UID(name + "-uid"),
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClassName,
			VolumeName:       volumeName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func pv(name, namespace, claimName string, reclaimPolicy corev1.PersistentVolumeReclaimPolicy) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{}},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: reclaimPolicy,
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: namespace,
				Name:      claimName,
			},
		},
	}
}

func migrationNames(migrations []*Migration) []string {
	names := make([]string, 0, len(migrations))
	for _, migration := range migrations {
		names = append(names, migration.Source.Name)
	}
	return names
}

func succeedCreatedPods(client *fake.Clientset, created *[]corev1.Pod) {
	client.Fake.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		create := action.(ktesting.CreateAction)
		pod := create.GetObject().(*corev1.Pod).DeepCopy()
		pod.Status.Phase = corev1.PodSucceeded
		if created != nil {
			*created = append(*created, *pod.DeepCopy())
		}
		err := client.Tracker().Create(action.GetResource(), pod, action.GetNamespace(), metav1.CreateOptions{})
		return true, pod, err
	})
}

func bindCreatedPVCs(client *fake.Clientset) {
	client.Fake.PrependReactor("create", "persistentvolumeclaims", func(action ktesting.Action) (bool, runtime.Object, error) {
		create := action.(ktesting.CreateAction)
		pvc := create.GetObject().(*corev1.PersistentVolumeClaim).DeepCopy()
		if pvc.Spec.VolumeName != "" {
			pvc.Status.Phase = corev1.ClaimBound
		}
		err := client.Tracker().Create(action.GetResource(), pvc, action.GetNamespace(), metav1.CreateOptions{})
		return true, pvc, err
	})
}

func bindCreatedPVCsToPV(client *fake.Clientset, pvName string) {
	client.Fake.PrependReactor("create", "persistentvolumeclaims", func(action ktesting.Action) (bool, runtime.Object, error) {
		create := action.(ktesting.CreateAction)
		pvc := create.GetObject().(*corev1.PersistentVolumeClaim).DeepCopy()
		if pvc.Labels[LabelRole] == "destination" && pvc.Spec.VolumeName == "" {
			pvc.Spec.VolumeName = pvName
		}
		if pvc.Spec.VolumeName != "" {
			pvc.Status.Phase = corev1.ClaimBound
		}
		err := client.Tracker().Create(action.GetResource(), pvc, action.GetNamespace(), metav1.CreateOptions{})
		return true, pvc, err
	})
}

func syncPodPhases(pods []corev1.Pod) []string {
	phases := make([]string, 0, len(pods))
	for _, pod := range pods {
		if strings.Contains(pod.Name, "-initial-") {
			phases = append(phases, "initial")
			continue
		}
		if strings.Contains(pod.Name, "-final-") {
			phases = append(phases, "final")
			continue
		}
		phases = append(phases, pod.Name)
	}
	return phases
}
