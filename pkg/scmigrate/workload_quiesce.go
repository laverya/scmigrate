package scmigrate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type workloadConsumers struct {
	ref  WorkloadRef
	pods []corev1.Pod
}

func (r *Runner) quiesce(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	current, err := r.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if err == nil {
		pvc = current
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	var records []QuiesceRecord
	if raw := pvc.Annotations[AnnQuiesce]; raw != "" {
		records, err = quiesceRecordsFromAnnotation(raw)
		if err != nil {
			return err
		}
		if err := validateQuiesceRecords(records, pvc.Namespace); err != nil {
			return err
		}
	} else {
		consumers, err := r.pvcConsumers(ctx, pvc)
		if err != nil {
			return err
		}
		records, err = r.quiesceConsumerGroups(ctx, pvc, consumers)
		if err != nil {
			return err
		}
	}
	if err := r.storeQuiesceRecords(ctx, pvc, records); err != nil {
		return err
	}
	if err := r.applyQuiesceRecords(ctx, records); err != nil {
		return err
	}
	if r.opts.DryRun {
		return nil
	}
	return r.waitNoActiveConsumers(ctx, pvc)
}

func (r *Runner) quiesceConsumerGroups(ctx context.Context, pvc *corev1.PersistentVolumeClaim, consumers []PVCConsumer) ([]QuiesceRecord, error) {
	if len(consumers) == 0 {
		return []QuiesceRecord{{Kind: WorkloadKindNone, Namespace: pvc.Namespace}}, nil
	}
	groups := groupPVCConsumers(consumers)
	records := make([]QuiesceRecord, 0, len(groups))
	for _, group := range groups {
		record, err := r.quiesceWorkloadGroup(ctx, pvc, group)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (r *Runner) quiesceWorkloadGroup(ctx context.Context, pvc *corev1.PersistentVolumeClaim, group workloadConsumers) (QuiesceRecord, error) {
	switch group.ref.Kind {
	case WorkloadKindPod:
		return r.quiesceStandalonePods(ctx, group.ref, group.pods)
	case WorkloadKindDeployment:
		return r.quiesceDeploymentConsumers(ctx, pvc, group.ref, group.pods)
	case WorkloadKindReplicaSet:
		return r.quiesceReplicaSetConsumers(ctx, group.ref, group.pods)
	case WorkloadKindStatefulSet:
		return r.quiesceStatefulSetConsumers(ctx, pvc, group.ref, group.pods)
	case WorkloadKindDaemonSet:
		return r.quiesceDaemonSetConsumers(ctx, group.ref, group.pods)
	default:
		return QuiesceRecord{}, fmt.Errorf("unsupported pod controller %s/%s for pvc/%s", group.ref.Kind, group.ref.Name, pvc.Name)
	}
}

func (r *Runner) quiesceStandalonePods(ctx context.Context, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	names := podNames(pods)
	record := QuiesceRecord{Kind: WorkloadKindPod, Name: names[0], Namespace: ref.Namespace, PodName: names[0], PodNames: names, PodUIDs: podUIDs(pods)}
	return record, nil
}

func (r *Runner) quiesceDeploymentConsumers(ctx context.Context, pvc *corev1.PersistentVolumeClaim, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	deploy, err := r.client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return QuiesceRecord{}, err
	}
	if err := ensureUID("deployment", deploy.Name, ref.UID, deploy.UID); err != nil {
		return QuiesceRecord{}, err
	}
	replicas := int32(1)
	if deploy.Spec.Replicas != nil {
		replicas = *deploy.Spec.Replicas
	}
	if replicas < 1 {
		return QuiesceRecord{}, fmt.Errorf("deployment/%s has no replicas to quiesce", deploy.Name)
	}
	names := podNames(pods)
	record := QuiesceRecord{Kind: WorkloadKindDeployment, Name: deploy.Name, Namespace: ref.Namespace, WorkloadUID: deploy.UID, OriginalReplicas: replicas, PodName: names[0], PodNames: names}
	return record, nil
}

func (r *Runner) quiesceReplicaSetConsumers(ctx context.Context, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	rs, err := r.client.AppsV1().ReplicaSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return QuiesceRecord{}, err
	}
	if err := ensureUID("replicaset", rs.Name, ref.UID, rs.UID); err != nil {
		return QuiesceRecord{}, err
	}
	return r.quiesceReplicaSetObject(ctx, ref, pods, rs)
}

func (r *Runner) quiesceReplicaSetObject(ctx context.Context, ref WorkloadRef, pods []corev1.Pod, rs *appsv1.ReplicaSet) (QuiesceRecord, error) {
	replicas := int32(1)
	if rs.Spec.Replicas != nil {
		replicas = *rs.Spec.Replicas
	}
	if replicas < 1 {
		return QuiesceRecord{}, fmt.Errorf("replicaset/%s has no replicas to quiesce", rs.Name)
	}
	names := podNames(pods)
	record := QuiesceRecord{Kind: WorkloadKindReplicaSet, Name: rs.Name, Namespace: ref.Namespace, WorkloadUID: rs.UID, OriginalReplicas: replicas, PodName: names[0], PodNames: names}
	return record, nil
}

func (r *Runner) quiesceStatefulSetConsumers(ctx context.Context, pvc *corev1.PersistentVolumeClaim, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	sts, err := r.client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return QuiesceRecord{}, err
	}
	if err := ensureUID("statefulset", sts.Name, ref.UID, sts.UID); err != nil {
		return QuiesceRecord{}, err
	}
	if len(pods) == 1 {
		return r.quiesceStatefulSet(ctx, pvc, &pods[0], sts)
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if replicas < 1 {
		return QuiesceRecord{}, fmt.Errorf("statefulset/%s has no replicas to quiesce", sts.Name)
	}
	names := podNames(pods)
	record := QuiesceRecord{Kind: WorkloadKindStatefulSet, Name: sts.Name, Namespace: ref.Namespace, WorkloadUID: sts.UID, OriginalReplicas: replicas, PodName: names[0], PodNames: names}
	return record, nil
}

func (r *Runner) quiesceStatefulSet(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pod *corev1.Pod, sts *appsv1.StatefulSet) (QuiesceRecord, error) {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	ordinal, ok := podOrdinal(pod.Name)
	if !ok {
		return QuiesceRecord{}, fmt.Errorf("cannot infer ordinal for statefulset pod/%s", pod.Name)
	}
	if ordinal >= replicas {
		return QuiesceRecord{}, fmt.Errorf("statefulset/%s pod/%s ordinal %d is outside current replica count %d", sts.Name, pod.Name, ordinal, replicas)
	}
	record := QuiesceRecord{Kind: WorkloadKindStatefulSet, Name: sts.Name, Namespace: pod.Namespace, WorkloadUID: sts.UID, OriginalReplicas: replicas, PodName: pod.Name, PodNames: []string{pod.Name}, PodUIDs: podUIDs([]corev1.Pod{*pod})}
	if replicas > 1 {
		// Preserve the controller spec outside the PVC before orphaning it; a
		// multi-replica StatefulSet can then migrate one ordinal at a time.
		name, err := r.storeStatefulSetForRecreate(ctx, pvc, sts)
		if err != nil {
			return QuiesceRecord{}, err
		}
		record.StatefulSetConfigMap = name
	}
	return record, nil
}

func (r *Runner) quiesceDaemonSetConsumers(ctx context.Context, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	ds, err := r.client.AppsV1().DaemonSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return QuiesceRecord{}, err
	}
	if err := ensureUID("daemonset", ds.Name, ref.UID, ds.UID); err != nil {
		return QuiesceRecord{}, err
	}
	return r.quiesceDaemonSetObject(ctx, ref, pods, ds)
}

func (r *Runner) quiesceDaemonSetObject(ctx context.Context, ref WorkloadRef, pods []corev1.Pod, ds *appsv1.DaemonSet) (QuiesceRecord, error) {
	names := podNames(pods)
	nodes := podNodeNames(pods)
	if len(nodes) == 0 {
		return QuiesceRecord{}, fmt.Errorf("daemonset/%s has no assigned consumer nodes", ds.Name)
	}
	record := QuiesceRecord{
		Kind:              WorkloadKindDaemonSet,
		Name:              ds.Name,
		Namespace:         ref.Namespace,
		WorkloadUID:       ds.UID,
		PodName:           names[0],
		PodNames:          names,
		PodUIDs:           podUIDs(pods),
		NodeName:          nodes[0],
		NodeNames:         nodes,
		DaemonSetStrategy: ds.Spec.UpdateStrategy,
		DaemonSetAffinity: ds.Spec.Template.Spec.Affinity,
	}
	return record, nil
}

func groupPVCConsumers(consumers []PVCConsumer) []workloadConsumers {
	groupsByKey := map[string]*workloadConsumers{}
	for _, consumer := range consumers {
		key := workloadKey(consumer.Workload)
		group := groupsByKey[key]
		if group == nil {
			group = &workloadConsumers{ref: consumer.Workload}
			groupsByKey[key] = group
		}
		group.pods = append(group.pods, consumer.Pod)
	}

	keys := make([]string, 0, len(groupsByKey))
	for key := range groupsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	groups := make([]workloadConsumers, 0, len(keys))
	for _, key := range keys {
		group := groupsByKey[key]
		sort.SliceStable(group.pods, func(i, j int) bool {
			return group.pods[i].Name < group.pods[j].Name
		})
		groups = append(groups, *group)
	}
	return groups
}

func workloadKey(ref WorkloadRef) string {
	return ref.Namespace + "\x00" + ref.Kind + "\x00" + ref.Name + "\x00" + string(ref.UID)
}

func podNames(pods []corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	sort.Strings(names)
	return names
}

func podNodeNames(pods []corev1.Pod) []string {
	seen := map[string]bool{}
	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			seen[pod.Spec.NodeName] = true
		}
	}
	nodes := make([]string, 0, len(seen))
	for node := range seen {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

func statefulSetRecordName(pvc *corev1.PersistentVolumeClaim, sts *appsv1.StatefulSet) string {
	base := "scmigrate-sts-" + sts.Name + "-" + pvc.Name
	hash := shortHash(pvc.Namespace + "/" + pvc.Name + "/" + sts.Namespace + "/" + sts.Name + "/" + string(pvc.UID))
	maxBase := 63 - len(hash) - 1
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	return strings.TrimRight(base, "-") + "-" + hash
}

func (r *Runner) storeStatefulSetForRecreate(ctx context.Context, pvc *corev1.PersistentVolumeClaim, sts *appsv1.StatefulSet) (string, error) {
	name := statefulSetRecordName(pvc, sts)
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: create configmap/%s with statefulset/%s restore record\n", name, sts.Name)
		return name, nil
	}
	stored := statefulSetForRecreate(sts)
	data, err := json.Marshal(stored)
	if err != nil {
		return "", err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: sts.Namespace,
			Labels: map[string]string{
				LabelManagedBy: ManagedByValue,
				LabelRole:      "statefulset-record",
				LabelSourceUID: string(pvc.UID),
			},
			Annotations: map[string]string{
				AnnSourcePVC:       pvc.Name,
				AnnSourceNamespace: pvc.Namespace,
			},
		},
		Data: map[string]string{ConfigMapKeyStatefulSet: string(data)},
	}
	if existing, err := r.client.CoreV1().ConfigMaps(sts.Namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return name, validateStatefulSetRecord(existing, cm)
	} else if !apierrors.IsNotFound(err) {
		return "", err
	}
	_, err = r.client.CoreV1().ConfigMaps(sts.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := r.client.CoreV1().ConfigMaps(sts.Namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			return "", getErr
		}
		return name, validateStatefulSetRecord(existing, cm)
	}
	if err != nil {
		return "", err
	}
	return name, nil
}

func validateStatefulSetRecord(existing, expected *corev1.ConfigMap) error {
	if existing.Labels[LabelManagedBy] != ManagedByValue || existing.Labels[LabelRole] != "statefulset-record" ||
		existing.Annotations[AnnSourcePVC] != expected.Annotations[AnnSourcePVC] ||
		existing.Annotations[AnnSourceNamespace] != expected.Annotations[AnnSourceNamespace] ||
		existing.Data[ConfigMapKeyStatefulSet] != expected.Data[ConfigMapKeyStatefulSet] {
		return fmt.Errorf("configmap/%s already exists without the expected StatefulSet restore record", existing.Name)
	}
	return nil
}

func statefulSetForRecreate(sts *appsv1.StatefulSet) *appsv1.StatefulSet {
	out := sts.DeepCopy()
	out.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"}
	out.ObjectMeta.ResourceVersion = ""
	out.ObjectMeta.UID = ""
	out.ObjectMeta.Generation = 0
	out.ObjectMeta.CreationTimestamp = metav1.Time{}
	out.ObjectMeta.DeletionTimestamp = nil
	out.ObjectMeta.DeletionGracePeriodSeconds = nil
	out.ObjectMeta.ManagedFields = nil
	out.ObjectMeta.OwnerReferences = nil
	out.Status = appsv1.StatefulSetStatus{}
	return out
}
