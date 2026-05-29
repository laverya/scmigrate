package scmigrate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

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
	case WorkloadKindNone:
		return nil
	case WorkloadKindPod:
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
	case WorkloadKindDeployment:
		if r.opts.DryRun {
			fmt.Fprintf(r.out, "dry-run: scale deployment/%s from %d to 0 for %d pod(s)\n", record.Name, record.OriginalReplicas, len(recordPodNames(record)))
			return nil
		}
		return r.scaleDeployment(ctx, record.Namespace, record.Name, 0)
	case WorkloadKindReplicaSet:
		if r.opts.DryRun {
			fmt.Fprintf(r.out, "dry-run: scale replicaset/%s from %d to 0 for %d pod(s)\n", record.Name, record.OriginalReplicas, len(recordPodNames(record)))
			return nil
		}
		return r.scaleReplicaSet(ctx, record.Namespace, record.Name, 0)
	case WorkloadKindStatefulSet:
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
	case WorkloadKindDaemonSet:
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
		// OnDelete prevents the scheduling patch from rolling every DaemonSet pod;
		// only pods on nodes using this PVC are deleted.
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
