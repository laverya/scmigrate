package scmigrate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

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
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get destination PVC while storing quiesce record: %w", err)
	}
	return nil
}

func (r *Runner) restoreWorkload(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	current, err := r.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("refresh pvc/%s before workload restore: %w", pvc.Name, err)
	}
	pvc = current
	recordRaw := pvc.Annotations[AnnQuiesce]
	if recordRaw == "" {
		return r.patchPVCAnnotations(ctx, pvc, map[string]string{AnnState: StateRestored})
	}
	records, err := quiesceRecordsFromAnnotation(recordRaw)
	if err != nil {
		return err
	}
	if err := validateQuiesceRecords(records, pvc.Namespace); err != nil {
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
	case WorkloadKindNone:
	case WorkloadKindPod:
		names := recordPodNames(record)
		fmt.Fprintf(r.out, "standalone pod(s) %s were deleted; recreate them manually if needed\n", strings.Join(names, ","))
	case WorkloadKindDeployment:
		if err := r.scaleDeployment(ctx, record.Namespace, record.Name, record.OriginalReplicas, record.WorkloadUID); err != nil {
			return err
		}
	case WorkloadKindReplicaSet:
		if err := r.scaleReplicaSet(ctx, record.Namespace, record.Name, record.OriginalReplicas, record.WorkloadUID); err != nil {
			return err
		}
	case WorkloadKindStatefulSet:
		if record.StatefulSetConfigMap != "" || record.StatefulSet != nil {
			if err := r.restoreOrphanedStatefulSet(ctx, record); err != nil {
				return err
			}
		} else {
			if err := r.scaleStatefulSet(ctx, record.Namespace, record.Name, record.OriginalReplicas, record.WorkloadUID); err != nil {
				return err
			}
		}
	case WorkloadKindDaemonSet:
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
	if apierrors.IsAlreadyExists(err) {
		existing, getErr := r.client.AppsV1().StatefulSets(sts.Namespace).Get(ctx, sts.Name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if !apiequality.Semantic.DeepEqual(existing.Spec, sts.Spec) {
			return fmt.Errorf("statefulset/%s already exists with a different spec than the restore record", sts.Name)
		}
		return nil
	}
	if err != nil {
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
		return validateStatefulSetForRecord(&sts, record)
	}
	if record.StatefulSet == nil {
		return nil, fmt.Errorf("statefulset quiesce record for %s/%s has no restore object", record.Namespace, record.Name)
	}
	return validateStatefulSetForRecord(record.StatefulSet, record)
}

func validateStatefulSetForRecord(sts *appsv1.StatefulSet, record QuiesceRecord) (*appsv1.StatefulSet, error) {
	if sts.Namespace != record.Namespace || sts.Name != record.Name {
		return nil, fmt.Errorf("StatefulSet restore object is for %s/%s, expected %s/%s", sts.Namespace, sts.Name, record.Namespace, record.Name)
	}
	return sts, nil
}

func (r *Runner) restoreDaemonSet(ctx context.Context, record QuiesceRecord) error {
	strategy := record.DaemonSetStrategy
	if strategy.Type == "" {
		strategy.Type = appsv1.RollingUpdateDaemonSetStrategyType
	}
	ds, err := r.client.AppsV1().DaemonSets(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := ensureUID("daemonset", ds.Name, record.WorkloadUID, ds.UID); err != nil {
		return err
	}
	data, err := jsonPatchForObject(
		ds,
		jsonPatchOperation{Op: "add", Path: "/spec/updateStrategy", Value: strategy},
		jsonPatchOperation{Op: "add", Path: "/spec/template/spec/affinity", Value: record.DaemonSetAffinity},
	)
	if err != nil {
		return err
	}
	_, err = r.client.AppsV1().DaemonSets(record.Namespace).Patch(ctx, record.Name, types.JSONPatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) waitWorkloadRestored(ctx context.Context, record QuiesceRecord) error {
	return wait(ctx, 10*time.Minute, func() (bool, error) {
		switch record.Kind {
		case WorkloadKindNone, WorkloadKindPod:
			return true, nil
		case WorkloadKindDeployment:
			deploy, err := r.client.AppsV1().Deployments(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if err := ensureUID("deployment", deploy.Name, record.WorkloadUID, deploy.UID); err != nil {
				return false, err
			}
			return deploy.Status.ReadyReplicas >= record.OriginalReplicas, nil
		case WorkloadKindReplicaSet:
			rs, err := r.client.AppsV1().ReplicaSets(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if err := ensureUID("replicaset", rs.Name, record.WorkloadUID, rs.UID); err != nil {
				return false, err
			}
			return rs.Status.ReadyReplicas >= record.OriginalReplicas, nil
		case WorkloadKindStatefulSet:
			sts, err := r.client.AppsV1().StatefulSets(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if record.StatefulSetConfigMap == "" && record.StatefulSet == nil {
				if err := ensureUID("statefulset", sts.Name, record.WorkloadUID, sts.UID); err != nil {
					return false, err
				}
			}
			return sts.Status.ReadyReplicas >= record.OriginalReplicas, nil
		case WorkloadKindDaemonSet:
			ds, err := r.client.AppsV1().DaemonSets(record.Namespace).Get(ctx, record.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if err := ensureUID("daemonset", ds.Name, record.WorkloadUID, ds.UID); err != nil {
				return false, err
			}
			nodes := recordNodeNames(record)
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
		owner := controllerOwner(pod.OwnerReferences)
		owned := owner != nil && owner.Kind == WorkloadKindDaemonSet && owner.Name == ds.Name && (owner.UID == "" || ds.UID == "" || owner.UID == ds.UID)
		if owned && pod.Spec.NodeName == nodeName && podReady(pod) {
			return true, nil
		}
	}
	return false, nil
}
