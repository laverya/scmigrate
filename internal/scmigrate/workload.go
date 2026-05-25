package scmigrate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type workloadConsumers struct {
	ref  WorkloadRef
	pods []corev1.Pod
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
	return ref.Namespace + "\x00" + ref.Kind + "\x00" + ref.Name
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
	return r.waitNoRunningConsumers(ctx, pvc)
}

func (r *Runner) quiescePod(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pod *corev1.Pod) (QuiesceRecord, error) {
	ref, err := r.workloadRefForPod(ctx, pod)
	if err != nil {
		return QuiesceRecord{}, err
	}
	records, err := r.quiesceConsumerGroups(ctx, pvc, []PVCConsumer{{Pod: *pod, Workload: ref}})
	if err != nil {
		return QuiesceRecord{}, err
	}
	if len(records) == 0 {
		return QuiesceRecord{}, fmt.Errorf("pod/%s did not produce a quiesce record", pod.Name)
	}
	return records[0], nil
}

func (r *Runner) quiesceDeployment(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pod *corev1.Pod, name string) (QuiesceRecord, error) {
	return r.quiesceDeploymentConsumers(ctx, pvc, WorkloadRef{Kind: "Deployment", Namespace: pod.Namespace, Name: name}, []corev1.Pod{*pod})
}

func (r *Runner) quiesceConsumerGroups(ctx context.Context, pvc *corev1.PersistentVolumeClaim, consumers []PVCConsumer) ([]QuiesceRecord, error) {
	if len(consumers) == 0 {
		return []QuiesceRecord{{Kind: "None", Namespace: pvc.Namespace}}, nil
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
	case "Pod":
		return r.quiesceStandalonePods(ctx, group.ref, group.pods)
	case "Deployment":
		return r.quiesceDeploymentConsumers(ctx, pvc, group.ref, group.pods)
	case "ReplicaSet":
		return r.quiesceReplicaSetConsumers(ctx, group.ref, group.pods)
	case "StatefulSet":
		return r.quiesceStatefulSetConsumers(ctx, pvc, group.ref, group.pods)
	case "DaemonSet":
		return r.quiesceDaemonSetConsumers(ctx, group.ref, group.pods)
	default:
		return QuiesceRecord{}, fmt.Errorf("unsupported pod controller %s/%s for pvc/%s", group.ref.Kind, group.ref.Name, pvc.Name)
	}
}

func (r *Runner) quiesceStandalonePods(ctx context.Context, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	names := podNames(pods)
	record := QuiesceRecord{Kind: "Pod", Name: names[0], Namespace: ref.Namespace, PodName: names[0], PodNames: names}
	return record, nil
}

func (r *Runner) quiesceDeploymentConsumers(ctx context.Context, pvc *corev1.PersistentVolumeClaim, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	deploy, err := r.client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
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
	record := QuiesceRecord{Kind: "Deployment", Name: deploy.Name, Namespace: ref.Namespace, OriginalReplicas: replicas, PodName: names[0], PodNames: names}
	return record, nil
}

func (r *Runner) quiesceReplicaSet(ctx context.Context, pod *corev1.Pod, rs *appsv1.ReplicaSet) (QuiesceRecord, error) {
	return r.quiesceReplicaSetObject(ctx, WorkloadRef{Kind: "ReplicaSet", Namespace: pod.Namespace, Name: rs.Name}, []corev1.Pod{*pod}, rs)
}

func (r *Runner) quiesceReplicaSetConsumers(ctx context.Context, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	rs, err := r.client.AppsV1().ReplicaSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
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
	record := QuiesceRecord{Kind: "ReplicaSet", Name: rs.Name, Namespace: ref.Namespace, OriginalReplicas: replicas, PodName: names[0], PodNames: names}
	return record, nil
}

func (r *Runner) quiesceStatefulSetConsumers(ctx context.Context, pvc *corev1.PersistentVolumeClaim, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	sts, err := r.client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
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
	record := QuiesceRecord{Kind: "StatefulSet", Name: sts.Name, Namespace: ref.Namespace, OriginalReplicas: replicas, PodName: names[0], PodNames: names}
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
	record := QuiesceRecord{Kind: "StatefulSet", Name: sts.Name, Namespace: pod.Namespace, OriginalReplicas: replicas, PodName: pod.Name, PodNames: []string{pod.Name}}
	if replicas > 1 {
		name, err := r.storeStatefulSetForRecreate(ctx, pvc, sts)
		if err != nil {
			return QuiesceRecord{}, err
		}
		record.StatefulSetConfigMap = name
	}
	return record, nil
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
	if _, err := r.client.CoreV1().ConfigMaps(sts.Namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		return name, nil
	} else if !apierrors.IsNotFound(err) {
		return "", err
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
				LabelManagedBy: "scmigrate",
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
	_, err = r.client.CoreV1().ConfigMaps(sts.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}
	return name, nil
}

func (r *Runner) orphanStatefulSetAndDeletePod(ctx context.Context, pod *corev1.Pod, sts *appsv1.StatefulSet) error {
	propagation := metav1.DeletePropagationOrphan
	err := r.client.AppsV1().StatefulSets(sts.Namespace).Delete(ctx, sts.Name, metav1.DeleteOptions{PropagationPolicy: &propagation})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.waitStatefulSetGone(ctx, sts.Namespace, sts.Name); err != nil {
		return err
	}
	err = r.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
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

func (r *Runner) quiesceDaemonSet(ctx context.Context, pod *corev1.Pod, ds *appsv1.DaemonSet) (QuiesceRecord, error) {
	return r.quiesceDaemonSetObject(ctx, WorkloadRef{Kind: "DaemonSet", Namespace: pod.Namespace, Name: ds.Name}, []corev1.Pod{*pod}, ds)
}

func (r *Runner) quiesceDaemonSetConsumers(ctx context.Context, ref WorkloadRef, pods []corev1.Pod) (QuiesceRecord, error) {
	ds, err := r.client.AppsV1().DaemonSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
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
		Kind:              "DaemonSet",
		Name:              ds.Name,
		Namespace:         ref.Namespace,
		PodName:           names[0],
		PodNames:          names,
		NodeName:          nodes[0],
		NodeNames:         nodes,
		DaemonSetStrategy: ds.Spec.UpdateStrategy,
		DaemonSetAffinity: ds.Spec.Template.Spec.Affinity,
	}
	return record, nil
}

func (r *Runner) applyQuiesceRecords(ctx context.Context, records []QuiesceRecord) error {
	for _, record := range records {
		if err := r.applyQuiesceRecord(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) applyQuiesceRecord(ctx context.Context, record QuiesceRecord) error {
	switch record.Kind {
	case "None":
		return nil
	case "Pod":
		names := recordPodNames(record)
		if r.opts.DryRun {
			fmt.Fprintf(r.out, "dry-run: delete standalone pod(s) %s\n", strings.Join(names, ","))
			return nil
		}
		for _, name := range names {
			err := r.client.CoreV1().Pods(record.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	case "Deployment":
		if r.opts.DryRun {
			fmt.Fprintf(r.out, "dry-run: scale deployment/%s from %d to 0 for %d pod(s)\n", record.Name, record.OriginalReplicas, len(recordPodNames(record)))
			return nil
		}
		return r.scaleDeployment(ctx, record.Namespace, record.Name, 0)
	case "ReplicaSet":
		if r.opts.DryRun {
			fmt.Fprintf(r.out, "dry-run: scale replicaset/%s from %d to 0 for %d pod(s)\n", record.Name, record.OriginalReplicas, len(recordPodNames(record)))
			return nil
		}
		return r.scaleReplicaSet(ctx, record.Namespace, record.Name, 0)
	case "StatefulSet":
		if record.StatefulSetConfigMap != "" || record.StatefulSet != nil {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: firstPodName(record), Namespace: record.Namespace}}
			if r.opts.DryRun {
				fmt.Fprintf(r.out, "dry-run: orphan statefulset/%s, delete pod/%s, recreate statefulset/%s\n", record.Name, pod.Name, record.Name)
				return nil
			}
			sts, err := r.statefulSetForRecord(ctx, record)
			if err != nil {
				return err
			}
			return r.orphanStatefulSetAndDeletePod(ctx, pod, sts)
		}
		targetReplicas := int32(0)
		if len(recordPodNames(record)) <= 1 && record.OriginalReplicas > 0 {
			targetReplicas = record.OriginalReplicas - 1
		}
		if r.opts.DryRun {
			fmt.Fprintf(r.out, "dry-run: scale statefulset/%s from %d to %d\n", record.Name, record.OriginalReplicas, targetReplicas)
			return nil
		}
		return r.scaleStatefulSet(ctx, record.Namespace, record.Name, targetReplicas)
	case "DaemonSet":
		nodes := recordNodeNames(record)
		names := recordPodNames(record)
		if r.opts.DryRun {
			fmt.Fprintf(r.out, "dry-run: exclude node(s) %s from daemonset/%s and delete %d pod(s)\n", strings.Join(nodes, ","), record.Name, len(names))
			return nil
		}
		ds, err := r.client.AppsV1().DaemonSets(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if err := r.excludeDaemonSetNodes(ctx, ds, nodes); err != nil {
			return err
		}
		for _, name := range names {
			err := r.client.CoreV1().Pods(record.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported quiesce record kind %q", record.Kind)
	}
	return nil
}

func recordPodNames(record QuiesceRecord) []string {
	if len(record.PodNames) > 0 {
		return record.PodNames
	}
	if record.PodName != "" {
		return []string{record.PodName}
	}
	return nil
}

func recordNodeNames(record QuiesceRecord) []string {
	if len(record.NodeNames) > 0 {
		return record.NodeNames
	}
	if record.NodeName != "" {
		return []string{record.NodeName}
	}
	return nil
}

func firstPodName(record QuiesceRecord) string {
	names := recordPodNames(record)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func (r *Runner) excludeDaemonSetNode(ctx context.Context, ds *appsv1.DaemonSet, nodeName string) error {
	return r.excludeDaemonSetNodes(ctx, ds, []string{nodeName})
}

func (r *Runner) excludeDaemonSetNodes(ctx context.Context, ds *appsv1.DaemonSet, nodeNames []string) error {
	affinity := excludeNodesFromAffinity(ds.Spec.Template.Spec.Affinity, nodeNames)
	payload := map[string]any{
		"spec": map[string]any{
			"updateStrategy": map[string]any{
				"type":          string(appsv1.OnDeleteDaemonSetStrategyType),
				"rollingUpdate": nil,
			},
			"template": map[string]any{
				"spec": map[string]any{"affinity": affinity},
			},
		},
	}
	data, _ := json.Marshal(payload)
	_, err := r.client.AppsV1().DaemonSets(ds.Namespace).Patch(ctx, ds.Name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func excludeNodeFromAffinity(in *corev1.Affinity, nodeName string) *corev1.Affinity {
	return excludeNodesFromAffinity(in, []string{nodeName})
}

func excludeNodesFromAffinity(in *corev1.Affinity, nodeNames []string) *corev1.Affinity {
	affinity := &corev1.Affinity{}
	if in != nil {
		affinity = in.DeepCopy()
	}
	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	if affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
	}
	selector := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	values := append([]string{}, nodeNames...)
	sort.Strings(values)
	requirement := corev1.NodeSelectorRequirement{
		Key:      "metadata.name",
		Operator: corev1.NodeSelectorOpNotIn,
		Values:   values,
	}
	if len(selector.NodeSelectorTerms) == 0 {
		selector.NodeSelectorTerms = []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{requirement}}}
		return affinity
	}
	for i := range selector.NodeSelectorTerms {
		selector.NodeSelectorTerms[i].MatchFields = append(selector.NodeSelectorTerms[i].MatchFields, requirement)
	}
	return affinity
}

func (r *Runner) storeQuiesceRecord(ctx context.Context, pvc *corev1.PersistentVolumeClaim, record QuiesceRecord) error {
	return r.storeQuiesceAnnotation(ctx, pvc, record)
}

func (r *Runner) storeQuiesceRecords(ctx context.Context, pvc *corev1.PersistentVolumeClaim, records []QuiesceRecord) error {
	return r.storeQuiesceAnnotation(ctx, pvc, records)
}

func (r *Runner) storeQuiesceAnnotation(ctx context.Context, pvc *corev1.PersistentVolumeClaim, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := r.patchPVCAnnotations(ctx, pvc, map[string]string{AnnQuiesce: string(data)}); err != nil {
		return err
	}
	if dest, err := r.destinationPVC(ctx, pvc); err == nil {
		return r.patchPVCAnnotations(ctx, dest, map[string]string{AnnQuiesce: string(data)})
	}
	return nil
}

func (r *Runner) restoreWorkload(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	current, err := r.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if err == nil {
		pvc = current
	}
	recordRaw := pvc.Annotations[AnnQuiesce]
	if recordRaw == "" {
		return r.patchPVCAnnotations(ctx, pvc, map[string]string{AnnState: StateRestored})
	}
	records, err := quiesceRecordsFromAnnotation(recordRaw)
	if err != nil {
		return err
	}
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: restore %d workload record(s)\n", len(records))
		return nil
	}
	for _, record := range records {
		if err := r.restoreQuiesceRecord(ctx, record); err != nil {
			return err
		}
	}
	for _, record := range records {
		if err := r.waitWorkloadRestored(ctx, record); err != nil {
			return err
		}
	}
	return r.patchPVCAnnotations(ctx, pvc, map[string]string{AnnState: StateRestored})
}

func quiesceRecordsFromAnnotation(recordRaw string) ([]QuiesceRecord, error) {
	recordRaw = strings.TrimSpace(recordRaw)
	if recordRaw == "" {
		return nil, nil
	}
	if strings.HasPrefix(recordRaw, "[") {
		var records []QuiesceRecord
		if err := json.Unmarshal([]byte(recordRaw), &records); err != nil {
			return nil, err
		}
		return records, nil
	}
	var record QuiesceRecord
	if err := json.Unmarshal([]byte(recordRaw), &record); err != nil {
		return nil, err
	}
	return []QuiesceRecord{record}, nil
}

func (r *Runner) restoreQuiesceRecord(ctx context.Context, record QuiesceRecord) error {
	switch record.Kind {
	case "None":
	case "Pod":
		names := record.PodNames
		if len(names) == 0 && record.PodName != "" {
			names = []string{record.PodName}
		}
		fmt.Fprintf(r.out, "standalone pod(s) %s were deleted; recreate them manually if needed\n", strings.Join(names, ","))
	case "Deployment":
		if err := r.scaleDeployment(ctx, record.Namespace, record.Name, record.OriginalReplicas); err != nil {
			return err
		}
	case "ReplicaSet":
		if err := r.scaleReplicaSet(ctx, record.Namespace, record.Name, record.OriginalReplicas); err != nil {
			return err
		}
	case "StatefulSet":
		if record.StatefulSetConfigMap != "" || record.StatefulSet != nil {
			if err := r.restoreOrphanedStatefulSet(ctx, record); err != nil {
				return err
			}
		} else {
			if err := r.scaleStatefulSet(ctx, record.Namespace, record.Name, record.OriginalReplicas); err != nil {
				return err
			}
		}
	case "DaemonSet":
		if err := r.restoreDaemonSet(ctx, record); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported quiesce record kind %q", record.Kind)
	}
	return nil
}

func (r *Runner) restoreOrphanedStatefulSet(ctx context.Context, record QuiesceRecord) error {
	sts, err := r.statefulSetForRecord(ctx, record)
	if apierrors.IsNotFound(err) {
		if _, getErr := r.client.AppsV1().StatefulSets(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{}); getErr == nil {
			return nil
		}
	}
	if err != nil {
		return err
	}
	sts = statefulSetForRecreate(sts)
	_, err = r.client.AppsV1().StatefulSets(sts.Namespace).Create(ctx, sts, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (r *Runner) statefulSetForRecord(ctx context.Context, record QuiesceRecord) (*appsv1.StatefulSet, error) {
	if record.StatefulSetConfigMap != "" {
		cm, err := r.client.CoreV1().ConfigMaps(record.Namespace).Get(ctx, record.StatefulSetConfigMap, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		raw := cm.Data[ConfigMapKeyStatefulSet]
		if raw == "" {
			return nil, fmt.Errorf("configmap/%s is missing %s", cm.Name, ConfigMapKeyStatefulSet)
		}
		var sts appsv1.StatefulSet
		if err := json.Unmarshal([]byte(raw), &sts); err != nil {
			return nil, err
		}
		return &sts, nil
	}
	if record.StatefulSet == nil {
		return nil, fmt.Errorf("statefulset quiesce record for %s/%s has no restore object", record.Namespace, record.Name)
	}
	return record.StatefulSet, nil
}

func (r *Runner) scaleDeployment(ctx context.Context, namespace, name string, replicas int32) error {
	payload := map[string]any{"spec": map[string]any{"replicas": replicas}}
	data, _ := json.Marshal(payload)
	_, err := r.client.AppsV1().Deployments(namespace).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) scaleReplicaSet(ctx context.Context, namespace, name string, replicas int32) error {
	payload := map[string]any{"spec": map[string]any{"replicas": replicas}}
	data, _ := json.Marshal(payload)
	_, err := r.client.AppsV1().ReplicaSets(namespace).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) scaleStatefulSet(ctx context.Context, namespace, name string, replicas int32) error {
	payload := map[string]any{"spec": map[string]any{"replicas": replicas}}
	data, _ := json.Marshal(payload)
	_, err := r.client.AppsV1().StatefulSets(namespace).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) waitStatefulSetGone(ctx context.Context, namespace, name string) error {
	return wait(ctx, 10*time.Minute, func() (bool, error) {
		_, err := r.client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func (r *Runner) waitNoRunningConsumers(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	return wait(ctx, 10*time.Minute, func() (bool, error) {
		consumers, err := r.consumingPods(ctx, pvc)
		if err != nil {
			return false, err
		}
		return len(consumers) == 0, nil
	})
}

func (r *Runner) restoreDaemonSet(ctx context.Context, record QuiesceRecord) error {
	strategy := record.DaemonSetStrategy
	if strategy.Type == "" {
		strategy.Type = appsv1.RollingUpdateDaemonSetStrategyType
	}
	payload := map[string]any{
		"spec": map[string]any{
			"updateStrategy": strategy,
			"template": map[string]any{
				"spec": map[string]any{"affinity": record.DaemonSetAffinity},
			},
		},
	}
	data, _ := json.Marshal(payload)
	_, err := r.client.AppsV1().DaemonSets(record.Namespace).Patch(ctx, record.Name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) waitWorkloadRestored(ctx context.Context, record QuiesceRecord) error {
	return wait(ctx, 10*time.Minute, func() (bool, error) {
		switch record.Kind {
		case "None", "Pod":
			return true, nil
		case "Deployment":
			deploy, err := r.client.AppsV1().Deployments(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return deploy.Status.ReadyReplicas >= record.OriginalReplicas, nil
		case "ReplicaSet":
			rs, err := r.client.AppsV1().ReplicaSets(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return rs.Status.ReadyReplicas >= record.OriginalReplicas, nil
		case "StatefulSet":
			sts, err := r.client.AppsV1().StatefulSets(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return sts.Status.ReadyReplicas >= record.OriginalReplicas, nil
		case "DaemonSet":
			nodes := record.NodeNames
			if len(nodes) == 0 && record.NodeName != "" {
				nodes = []string{record.NodeName}
			}
			for _, node := range nodes {
				ok, err := r.daemonSetPodReadyOnNode(ctx, record.Namespace, record.Name, node)
				if err != nil || !ok {
					return ok, err
				}
			}
			return true, nil
		default:
			return false, fmt.Errorf("unsupported quiesce record kind %q", record.Kind)
		}
	})
}

func (r *Runner) daemonSetPodReadyOnNode(ctx context.Context, namespace, name, nodeName string) (bool, error) {
	ds, err := r.client.AppsV1().DaemonSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return false, err
	}
	pods, err := r.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return false, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName == nodeName && podReady(pod) {
			return true, nil
		}
	}
	return false, nil
}

func podReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func controllerOwner(refs []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}

func podOrdinal(name string) (int32, bool) {
	idx := strings.LastIndex(name, "-")
	if idx < 0 || idx == len(name)-1 {
		return 0, false
	}
	ordinal, err := strconv.Atoi(name[idx+1:])
	if err != nil {
		return 0, false
	}
	return int32(ordinal), true
}
