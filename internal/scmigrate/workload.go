package scmigrate

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const podDeletionCostAnnotation = "controller.kubernetes.io/pod-deletion-cost"

func (r *Runner) quiesce(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	consumers, err := r.consumingPods(ctx, pvc)
	if err != nil {
		return err
	}
	if len(consumers) == 0 {
		record := QuiesceRecord{Kind: "None", Namespace: pvc.Namespace}
		return r.storeQuiesceRecord(ctx, pvc, record)
	}
	if len(consumers) > 1 && !r.opts.AllowMultipleConsumers {
		return fmt.Errorf("PVC is mounted by %d running pods; refuse to finalize because all writers must be quiet. Use --allow-multiple-consumers only after externally quiescing them", len(consumers))
	}
	pod := consumers[0]
	record, err := r.quiescePod(ctx, pvc, &pod)
	if err != nil {
		return err
	}
	if err := r.storeQuiesceRecord(ctx, pvc, record); err != nil {
		return err
	}
	if r.opts.DryRun {
		return nil
	}
	return r.waitPodGone(ctx, pod.Namespace, pod.Name)
}

func (r *Runner) quiescePod(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pod *corev1.Pod) (QuiesceRecord, error) {
	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil {
		record := QuiesceRecord{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace, PodName: pod.Name}
		if r.opts.DryRun {
			fmt.Fprintf(r.out, "dry-run: delete standalone pod/%s\n", pod.Name)
			return record, nil
		}
		err := r.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return record, err
		}
		return record, nil
	}

	switch owner.Kind {
	case "ReplicaSet":
		rs, err := r.client.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return QuiesceRecord{}, err
		}
		if deployOwner := controllerOwner(rs.OwnerReferences); deployOwner != nil && deployOwner.Kind == "Deployment" {
			return r.quiesceDeployment(ctx, pvc, pod, deployOwner.Name)
		}
		return r.quiesceReplicaSet(ctx, pod, rs)
	case "StatefulSet":
		sts, err := r.client.AppsV1().StatefulSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return QuiesceRecord{}, err
		}
		return r.quiesceStatefulSet(ctx, pod, sts)
	case "DaemonSet":
		ds, err := r.client.AppsV1().DaemonSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return QuiesceRecord{}, err
		}
		return r.quiesceDaemonSet(ctx, pod, ds)
	default:
		return QuiesceRecord{}, fmt.Errorf("unsupported pod controller %s/%s for pod/%s", owner.Kind, owner.Name, pod.Name)
	}
}

func (r *Runner) quiesceDeployment(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pod *corev1.Pod, name string) (QuiesceRecord, error) {
	deploy, err := r.client.AppsV1().Deployments(pod.Namespace).Get(ctx, name, metav1.GetOptions{})
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
	record := QuiesceRecord{Kind: "Deployment", Name: deploy.Name, Namespace: pod.Namespace, OriginalReplicas: replicas, PodName: pod.Name}
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: scale deployment/%s from %d to %d for pod/%s\n", deploy.Name, replicas, replicas-1, pod.Name)
		return record, nil
	}
	if err := r.steerDeploymentDeletion(ctx, deploy, pod); err != nil {
		return record, err
	}
	return record, r.scaleDeployment(ctx, pod.Namespace, deploy.Name, replicas-1)
}

func (r *Runner) quiesceReplicaSet(ctx context.Context, pod *corev1.Pod, rs *appsv1.ReplicaSet) (QuiesceRecord, error) {
	replicas := int32(1)
	if rs.Spec.Replicas != nil {
		replicas = *rs.Spec.Replicas
	}
	if replicas < 1 {
		return QuiesceRecord{}, fmt.Errorf("replicaset/%s has no replicas to quiesce", rs.Name)
	}
	record := QuiesceRecord{Kind: "ReplicaSet", Name: rs.Name, Namespace: pod.Namespace, OriginalReplicas: replicas, PodName: pod.Name}
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: scale replicaset/%s from %d to %d for pod/%s\n", rs.Name, replicas, replicas-1, pod.Name)
		return record, nil
	}
	if err := r.patchPodDeletionCost(ctx, pod.Namespace, pod.Name, -1000); err != nil {
		return record, err
	}
	return record, r.scaleReplicaSet(ctx, pod.Namespace, rs.Name, replicas-1)
}

func (r *Runner) quiesceStatefulSet(ctx context.Context, pod *corev1.Pod, sts *appsv1.StatefulSet) (QuiesceRecord, error) {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	ordinal, ok := podOrdinal(pod.Name)
	if !ok {
		return QuiesceRecord{}, fmt.Errorf("cannot infer ordinal for statefulset pod/%s", pod.Name)
	}
	if ordinal != replicas-1 {
		return QuiesceRecord{}, fmt.Errorf("statefulset/%s can quiesce one pod safely only at highest ordinal; pod/%s is ordinal %d, current highest is %d. Migrate this StatefulSet in descending ordinal order", sts.Name, pod.Name, ordinal, replicas-1)
	}
	record := QuiesceRecord{Kind: "StatefulSet", Name: sts.Name, Namespace: pod.Namespace, OriginalReplicas: replicas, PodName: pod.Name}
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: scale statefulset/%s from %d to %d\n", sts.Name, replicas, replicas-1)
		return record, nil
	}
	return record, r.scaleStatefulSet(ctx, pod.Namespace, sts.Name, replicas-1)
}

func (r *Runner) quiesceDaemonSet(ctx context.Context, pod *corev1.Pod, ds *appsv1.DaemonSet) (QuiesceRecord, error) {
	if pod.Spec.NodeName == "" {
		return QuiesceRecord{}, fmt.Errorf("daemonset pod/%s has no assigned node", pod.Name)
	}
	record := QuiesceRecord{
		Kind:              "DaemonSet",
		Name:              ds.Name,
		Namespace:         pod.Namespace,
		PodName:           pod.Name,
		NodeName:          pod.Spec.NodeName,
		DaemonSetStrategy: ds.Spec.UpdateStrategy,
		DaemonSetAffinity: ds.Spec.Template.Spec.Affinity,
	}
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: exclude node/%s from daemonset/%s and delete pod/%s\n", pod.Spec.NodeName, ds.Name, pod.Name)
		return record, nil
	}
	if err := r.excludeDaemonSetNode(ctx, ds, pod.Spec.NodeName); err != nil {
		return record, err
	}
	err := r.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return record, err
	}
	return record, nil
}

func (r *Runner) steerDeploymentDeletion(ctx context.Context, deploy *appsv1.Deployment, target *corev1.Pod) error {
	selector, err := metav1.LabelSelectorAsSelector(deploy.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := r.client.CoreV1().Pods(deploy.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		cost := int32(1000)
		if pod.Name == target.Name {
			cost = -1000
		}
		if err := r.patchPodDeletionCost(ctx, pod.Namespace, pod.Name, cost); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) patchPodDeletionCost(ctx context.Context, namespace, name string, cost int32) error {
	payload := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{podDeletionCostAnnotation: strconv.Itoa(int(cost))},
		},
	}
	data, _ := json.Marshal(payload)
	_, err := r.client.CoreV1().Pods(namespace).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) excludeDaemonSetNode(ctx context.Context, ds *appsv1.DaemonSet, nodeName string) error {
	affinity := excludeNodeFromAffinity(ds.Spec.Template.Spec.Affinity, nodeName)
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
	requirement := corev1.NodeSelectorRequirement{
		Key:      "metadata.name",
		Operator: corev1.NodeSelectorOpNotIn,
		Values:   []string{nodeName},
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
	data, err := json.Marshal(record)
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
	var record QuiesceRecord
	if err := json.Unmarshal([]byte(recordRaw), &record); err != nil {
		return err
	}
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: restore workload %s/%s\n", record.Kind, record.Name)
		return nil
	}
	switch record.Kind {
	case "None":
	case "Pod":
		fmt.Fprintf(r.out, "standalone pod/%s was deleted; recreate it manually if needed\n", record.PodName)
	case "Deployment":
		if err := r.scaleDeployment(ctx, record.Namespace, record.Name, record.OriginalReplicas); err != nil {
			return err
		}
	case "ReplicaSet":
		if err := r.scaleReplicaSet(ctx, record.Namespace, record.Name, record.OriginalReplicas); err != nil {
			return err
		}
	case "StatefulSet":
		if err := r.scaleStatefulSet(ctx, record.Namespace, record.Name, record.OriginalReplicas); err != nil {
			return err
		}
	case "DaemonSet":
		if err := r.restoreDaemonSet(ctx, record); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported quiesce record kind %q", record.Kind)
	}
	if err := r.waitWorkloadRestored(ctx, record); err != nil {
		return err
	}
	return r.patchPVCAnnotations(ctx, pvc, map[string]string{AnnState: StateRestored})
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
			pod, err := r.client.CoreV1().Pods(record.Namespace).Get(ctx, record.PodName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return podReady(pod), nil
		case "DaemonSet":
			return r.daemonSetPodReadyOnNode(ctx, record.Namespace, record.Name, record.NodeName)
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
