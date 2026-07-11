package scmigrate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type jsonPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

func (r *Runner) patchPVCAnnotations(ctx context.Context, pvc *corev1.PersistentVolumeClaim, annotations map[string]string) error {
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: patch pvc/%s annotations %v\n", pvc.Name, sortedKeys(annotations))
		return nil
	}
	current, err := r.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pvc.UID != "" && current.UID != pvc.UID {
		return fmt.Errorf("pvc/%s was replaced: expected UID %q, found %q", pvc.Name, pvc.UID, current.UID)
	}
	merged := cloneMap(current.Annotations)
	for key, value := range annotations {
		merged[key] = value
	}
	data, err := jsonPatchForObject(current, jsonPatchOperation{Op: "add", Path: "/metadata/annotations", Value: merged})
	if err != nil {
		return err
	}
	_, err = r.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(ctx, pvc.Name, types.JSONPatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) waitPVCBound(ctx context.Context, namespace, name string, expectedUID types.UID) error {
	return wait(ctx, 20*time.Minute, func() (bool, error) {
		pvc, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if err := ensureUID("pvc", name, expectedUID, pvc.UID); err != nil {
			return false, err
		}
		return pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "", nil
	})
}

func (r *Runner) waitPVCGone(ctx context.Context, namespace, name string, expectedUID types.UID) error {
	return wait(ctx, 10*time.Minute, func() (bool, error) {
		pvc, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err == nil {
			err = ensureUID("pvc", name, expectedUID, pvc.UID)
		}
		return false, err
	})
}

func (r *Runner) waitPodGone(ctx context.Context, namespace, name string, expectedUID types.UID) error {
	return wait(ctx, 10*time.Minute, func() (bool, error) {
		pod, err := r.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err == nil {
			err = ensureUID("pod", name, expectedUID, pod.UID)
		}
		return false, err
	})
}

func (r *Runner) waitPodSucceeded(ctx context.Context, namespace, name string, expectedUID types.UID) error {
	return wait(ctx, 24*time.Hour, func() (bool, error) {
		pod, err := r.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if err := ensureUID("pod", name, expectedUID, pod.UID); err != nil {
			return false, err
		}
		if pod.Status.Phase == corev1.PodFailed {
			return false, syncPodFailure(pod)
		}
		return pod.Status.Phase == corev1.PodSucceeded, nil
	})
}

func syncPodFailure(pod *corev1.Pod) error {
	detail := strings.TrimSpace(pod.Status.Message)
	for _, status := range pod.Status.ContainerStatuses {
		terminated := status.State.Terminated
		if terminated == nil {
			continue
		}
		containerDetail := strings.TrimSpace(terminated.Message)
		if containerDetail == "" {
			containerDetail = strings.TrimSpace(terminated.Reason)
		}
		if containerDetail != "" {
			detail = fmt.Sprintf("container %s: %s", status.Name, containerDetail)
		}
		break
	}
	if detail == "" {
		return fmt.Errorf("pod/%s failed", pod.Name)
	}
	return fmt.Errorf("pod/%s failed: %s", pod.Name, detail)
}

func (r *Runner) deletePodByName(ctx context.Context, namespace, name string) error {
	return r.deletePodByNameWithUID(ctx, namespace, name, "")
}

func (r *Runner) deletePodByNameWithUID(ctx context.Context, namespace, name string, expectedUID types.UID) error {
	pod, err := r.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if expectedUID != "" && pod.UID != expectedUID {
		return fmt.Errorf("pod/%s was replaced: expected UID %q, found %q", name, expectedUID, pod.UID)
	}
	return r.deletePod(ctx, pod)
}

func (r *Runner) deletePod(ctx context.Context, pod *corev1.Pod) error {
	err := r.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, deleteOptionsForUID(pod.UID))
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (r *Runner) consumingPods(ctx context.Context, pvc *corev1.PersistentVolumeClaim) ([]corev1.Pod, error) {
	pods, err := r.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var consumers []corev1.Pod
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
				consumers = append(consumers, pod)
				break
			}
		}
	}
	return consumers, nil
}

func (r *Runner) ensureNoActiveConsumers(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	consumers, err := r.consumingPods(ctx, pvc)
	if err != nil {
		return err
	}
	if len(consumers) == 0 {
		return nil
	}
	return fmt.Errorf("pvc/%s still has active consumer pod(s): %v", pvc.Name, podNames(consumers))
}

func (r *Runner) pvcConsumers(ctx context.Context, pvc *corev1.PersistentVolumeClaim) ([]PVCConsumer, error) {
	pods, err := r.consumingPods(ctx, pvc)
	if err != nil {
		return nil, err
	}
	consumers := make([]PVCConsumer, 0, len(pods))
	for i := range pods {
		ref, err := r.workloadRefForPod(ctx, &pods[i])
		if err != nil {
			return nil, err
		}
		consumers = append(consumers, PVCConsumer{Pod: pods[i], Workload: ref})
	}
	sort.SliceStable(consumers, func(i, j int) bool {
		left := consumers[i]
		right := consumers[j]
		if left.Workload.Namespace != right.Workload.Namespace {
			return left.Workload.Namespace < right.Workload.Namespace
		}
		if left.Workload.Kind != right.Workload.Kind {
			return left.Workload.Kind < right.Workload.Kind
		}
		if left.Workload.Name != right.Workload.Name {
			return left.Workload.Name < right.Workload.Name
		}
		return left.Pod.Name < right.Pod.Name
	})
	return consumers, nil
}

func (r *Runner) workloadRefForPod(ctx context.Context, pod *corev1.Pod) (WorkloadRef, error) {
	owner := controllerOwner(pod.OwnerReferences)
	if owner == nil {
		return WorkloadRef{Kind: WorkloadKindPod, Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}, nil
	}

	switch owner.Kind {
	case WorkloadKindReplicaSet:
		rs, err := r.client.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return WorkloadRef{}, err
		}
		if err := ensureUID("replicaset", rs.Name, owner.UID, rs.UID); err != nil {
			return WorkloadRef{}, err
		}
		if deployOwner := controllerOwner(rs.OwnerReferences); deployOwner != nil && deployOwner.Kind == WorkloadKindDeployment {
			return WorkloadRef{Kind: WorkloadKindDeployment, Namespace: pod.Namespace, Name: deployOwner.Name, UID: deployOwner.UID}, nil
		}
		return WorkloadRef{Kind: WorkloadKindReplicaSet, Namespace: pod.Namespace, Name: owner.Name, UID: owner.UID}, nil
	case WorkloadKindStatefulSet, WorkloadKindDaemonSet:
		return WorkloadRef{Kind: owner.Kind, Namespace: pod.Namespace, Name: owner.Name, UID: owner.UID}, nil
	default:
		return WorkloadRef{}, fmt.Errorf("unsupported pod controller %s/%s for pod/%s", owner.Kind, owner.Name, pod.Name)
	}
}

func storageClass(pvc *corev1.PersistentVolumeClaim) string {
	if pvc.Spec.StorageClassName == nil {
		return ""
	}
	return *pvc.Spec.StorageClassName
}

func wait(ctx context.Context, timeout time.Duration, condition func() (bool, error)) error {
	deadline, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		done, err := condition()
		if done || err != nil {
			return err
		}
		select {
		case <-deadline.Done():
			return deadline.Err()
		case <-ticker.C:
		}
	}
}

func stateBefore(current, target string) (bool, error) {
	// Unknown states must stop the migration instead of being ranked as "new".
	currentRank, ok := migrationStateRank(current)
	if !ok {
		return false, fmt.Errorf("unknown migration state %q", current)
	}
	targetRank, ok := migrationStateRank(target)
	if !ok {
		return false, fmt.Errorf("unknown migration state %q", target)
	}
	return currentRank < targetRank, nil
}

func migrationStateRank(state string) (int, bool) {
	order := map[string]int{
		"":                 0,
		StateNew:           0,
		StatePrepared:      1,
		StateInitialSynced: 2,
		StateQuiesced:      3,
		StateFinalSynced:   4,
		StateCutover:       5,
		StateRestored:      6,
	}
	rank, ok := order[state]
	return rank, ok
}

func normalizeMigrationState(state string) (string, error) {
	if state == "" {
		state = StateNew
	}
	if _, ok := migrationStateRank(state); !ok {
		return "", fmt.Errorf("unknown migration state %q", state)
	}
	return state, nil
}

func ptrInt64(value int64) *int64 {
	return &value
}

func deleteOptionsForUID(uid types.UID) metav1.DeleteOptions {
	opts := metav1.DeleteOptions{}
	if uid != "" {
		uid := uid
		opts.Preconditions = &metav1.Preconditions{UID: &uid}
	}
	return opts
}

func deleteOptionsForUIDWithPropagation(uid types.UID, policy metav1.DeletionPropagation) metav1.DeleteOptions {
	opts := deleteOptionsForUID(uid)
	opts.PropagationPolicy = &policy
	return opts
}

func jsonPatchForObject(object metav1.Object, ops ...jsonPatchOperation) ([]byte, error) {
	preconditions := make([]jsonPatchOperation, 0, 2)
	if uid := object.GetUID(); uid != "" {
		preconditions = append(preconditions, jsonPatchOperation{
			Op:    "test",
			Path:  "/metadata/uid",
			Value: string(uid),
		})
	}
	if resourceVersion := object.GetResourceVersion(); resourceVersion != "" {
		preconditions = append(preconditions, jsonPatchOperation{
			Op:    "test",
			Path:  "/metadata/resourceVersion",
			Value: resourceVersion,
		})
	}
	ops = append(preconditions, ops...)
	return json.Marshal(ops)
}
