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
		fmt.Fprintf(r.out, "dry-run: restore workload %s/%s to %d replicas\n", record.Kind, record.Name, record.OriginalReplicas)
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
	default:
		return fmt.Errorf("unsupported quiesce record kind %q", record.Kind)
	}
	if err := wait(ctx, 10*time.Minute, func() (bool, error) {
		if record.PodName == "" || record.Kind == "None" || record.Kind == "Pod" {
			return true, nil
		}
		pod, err := r.client.CoreV1().Pods(record.Namespace).Get(ctx, record.PodName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return pod.Status.Phase == corev1.PodRunning, nil
	}); err != nil {
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
