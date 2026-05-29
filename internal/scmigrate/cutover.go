package scmigrate

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (r *Runner) cutover(ctx context.Context, source *corev1.PersistentVolumeClaim) error {
	namespace := source.Namespace
	name := source.Name
	source, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.resumeCutoverWithoutSource(ctx, namespace, name)
		}
		return err
	}
	dest, err := r.destinationPVC(ctx, source)
	if err != nil {
		return err
	}
	if dest.Spec.VolumeName == "" {
		if err := r.waitPVCBound(ctx, dest.Namespace, dest.Name); err != nil {
			return err
		}
		dest, err = r.destinationPVC(ctx, source)
		if err != nil {
			return err
		}
	}
	sourcePV, err := r.client.CoreV1().PersistentVolumes().Get(ctx, source.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	destPV, err := r.client.CoreV1().PersistentVolumes().Get(ctx, dest.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := r.retainPV(ctx, sourcePV, AnnOriginalSourceRP); err != nil {
		return err
	}
	if err := r.retainPV(ctx, destPV, AnnOriginalDestRP); err != nil {
		return err
	}
	if err := r.patchPVCAnnotations(ctx, dest, map[string]string{
		AnnState:         StateFinalSynced,
		AnnDestinationPV: dest.Spec.VolumeName,
		AnnSourcePV:      source.Spec.VolumeName,
	}); err != nil {
		return err
	}
	// After this point both PVC objects may be deleted, so the destination PV
	// carries the reconstruction record needed to resume cutover.
	if err := r.persistCutoverRecord(ctx, source, dest, destPV); err != nil {
		return err
	}

	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: delete pvc/%s and pvc/%s, clear pv/%s claimRef, create final pvc/%s bound to pv/%s\n", source.Name, dest.Name, dest.Spec.VolumeName, source.Name, dest.Spec.VolumeName)
		return nil
	}
	if err := r.client.CoreV1().PersistentVolumeClaims(source.Namespace).Delete(ctx, source.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.client.CoreV1().PersistentVolumeClaims(dest.Namespace).Delete(ctx, dest.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.waitPVCGone(ctx, source.Namespace, source.Name); err != nil {
		return err
	}
	if err := r.waitPVCGone(ctx, dest.Namespace, dest.Name); err != nil {
		return err
	}
	if err := r.clearPVClaimRef(ctx, dest.Spec.VolumeName); err != nil {
		return err
	}
	if _, err := r.createFinalPVC(ctx, dest.Annotations[AnnOriginalPVC], dest.Name, dest.Spec.VolumeName, quiesceRecord(source.Annotations, dest.Annotations)); err != nil {
		return err
	}
	if r.opts.RestoreReclaimPolicy {
		return r.restoreDestinationReclaimPolicy(ctx, dest.Spec.VolumeName)
	}
	return nil
}

func (r *Runner) resumeCutoverWithoutSource(ctx context.Context, namespace, name string) error {
	destName := ""
	pvcs, err := r.client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=destination", LabelManagedBy, ManagedByValue, LabelRole),
	})
	if err != nil {
		return err
	}
	for i := range pvcs.Items {
		if pvcs.Items[i].Annotations[AnnSourcePVC] == name {
			destName = pvcs.Items[i].Name
			break
		}
	}
	if destName == "" {
		if final, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{}); err == nil && final.Annotations[AnnState] == StateCutover {
			return nil
		}
		return r.resumeCutoverFromPV(ctx, namespace, name)
	}
	dest, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, destName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := r.validateTargetStorageClass("pvc/"+dest.Namespace+"/"+dest.Name, dest.Annotations); err != nil {
		return err
	}
	if dest.Spec.VolumeName == "" {
		return fmt.Errorf("destination pvc/%s is not bound", dest.Name)
	}
	destPV, err := r.client.CoreV1().PersistentVolumes().Get(ctx, dest.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	syntheticSource := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			UID:         types.UID(dest.Annotations[AnnSourceUID]),
			Annotations: map[string]string{AnnQuiesce: dest.Annotations[AnnQuiesce]},
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: dest.Annotations[AnnSourcePV]},
	}
	if err := r.persistCutoverRecord(ctx, syntheticSource, dest, destPV); err != nil {
		return err
	}
	if err := r.client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, dest.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.waitPVCGone(ctx, namespace, dest.Name); err != nil {
		return err
	}
	if err := r.clearPVClaimRef(ctx, dest.Spec.VolumeName); err != nil {
		return err
	}
	_, err = r.createFinalPVC(ctx, dest.Annotations[AnnOriginalPVC], dest.Name, dest.Spec.VolumeName, dest.Annotations[AnnQuiesce])
	return err
}

func (r *Runner) persistCutoverRecord(ctx context.Context, source, dest *corev1.PersistentVolumeClaim, destPV *corev1.PersistentVolume) error {
	annotations := map[string]string{
		AnnState:              StateFinalSynced,
		AnnSourcePVC:          source.Name,
		AnnSourceNamespace:    source.Namespace,
		AnnSourceUID:          string(source.UID),
		AnnSourcePV:           source.Spec.VolumeName,
		AnnDestinationPVC:     dest.Name,
		AnnDestinationPV:      destPV.Name,
		AnnTargetStorageClass: r.opts.TargetStorageClass,
		AnnOriginalPVC:        dest.Annotations[AnnOriginalPVC],
	}
	if quiesce := source.Annotations[AnnQuiesce]; quiesce != "" {
		annotations[AnnQuiesce] = quiesce
	} else if quiesce := dest.Annotations[AnnQuiesce]; quiesce != "" {
		annotations[AnnQuiesce] = quiesce
	}
	payload := map[string]any{"metadata": map[string]any{"annotations": annotations}}
	data, _ := json.Marshal(payload)
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: persist cutover record on pv/%s\n", destPV.Name)
		return nil
	}
	_, err := r.client.CoreV1().PersistentVolumes().Patch(ctx, destPV.Name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) resumeCutoverFromPV(ctx context.Context, namespace, name string) error {
	pvs, err := r.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range pvs.Items {
		pv := pvs.Items[i].DeepCopy()
		annotations := pv.Annotations
		if annotations[AnnSourceNamespace] != namespace || annotations[AnnSourcePVC] != name || annotations[AnnOriginalPVC] == "" {
			continue
		}
		if err := r.validateTargetStorageClass("pv/"+pv.Name, annotations); err != nil {
			return err
		}
		if err := r.clearPVClaimRef(ctx, pv.Name); err != nil {
			return err
		}
		if _, err := r.createFinalPVC(ctx, annotations[AnnOriginalPVC], annotations[AnnDestinationPVC], pv.Name, annotations[AnnQuiesce]); err != nil {
			return err
		}
		if r.opts.RestoreReclaimPolicy {
			return r.restoreDestinationReclaimPolicy(ctx, pv.Name)
		}
		return nil
	}
	return fmt.Errorf("source PVC is gone and no resumable destination PVC or PV cutover record for %s/%s was found", namespace, name)
}

func (r *Runner) createFinalPVC(ctx context.Context, originalPVC, destinationPVC, volumeName, quiesce string) (*corev1.PersistentVolumeClaim, error) {
	// All cutover and resume paths recreate the user-facing PVC from the same
	// stored source snapshot so labels, annotations, selectors, and size agree.
	stored, err := storedPVC(originalPVC)
	if err != nil {
		return nil, err
	}
	finalPVC, err := finalPVCFromStored(stored, r.opts.TargetStorageClass, volumeName)
	if err != nil {
		return nil, err
	}
	if quiesce != "" {
		finalPVC.Annotations[AnnQuiesce] = quiesce
	}
	finalPVC.Annotations[AnnDestinationPVC] = destinationPVC
	finalPVC.Annotations[AnnOriginalPVC] = originalPVC
	if _, err := r.client.CoreV1().PersistentVolumeClaims(finalPVC.Namespace).Create(ctx, finalPVC, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	if err := r.waitPVCBound(ctx, finalPVC.Namespace, finalPVC.Name); err != nil {
		return nil, err
	}
	return finalPVC, nil
}

func quiesceRecord(primary, fallback map[string]string) string {
	if primary[AnnQuiesce] != "" {
		return primary[AnnQuiesce]
	}
	return fallback[AnnQuiesce]
}

func (r *Runner) retainPV(ctx context.Context, pv *corev1.PersistentVolume, originalPolicyAnnotation string) error {
	annotations := map[string]string{
		originalPolicyAnnotation: string(pv.Spec.PersistentVolumeReclaimPolicy),
		AnnState:                 StateFinalSynced,
	}
	payload := map[string]any{
		"metadata": map[string]any{"annotations": annotations},
		"spec":     map[string]any{"persistentVolumeReclaimPolicy": string(corev1.PersistentVolumeReclaimRetain)},
	}
	data, _ := json.Marshal(payload)
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: set pv/%s reclaimPolicy=Retain\n", pv.Name)
		return nil
	}
	_, err := r.client.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (r *Runner) clearPVClaimRef(ctx context.Context, pvName string) error {
	patch := []byte(`[{"op":"remove","path":"/spec/claimRef"}]`)
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: clear pv/%s claimRef\n", pvName)
		return nil
	}
	pv, err := r.client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pv.Spec.ClaimRef == nil {
		return nil
	}
	_, err = r.client.CoreV1().PersistentVolumes().Patch(ctx, pvName, types.JSONPatchType, patch, metav1.PatchOptions{})
	if apierrors.IsInvalid(err) || apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *Runner) restoreDestinationReclaimPolicy(ctx context.Context, pvName string) error {
	pv, err := r.client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	policy := pv.Annotations[AnnOriginalDestRP]
	if policy == "" {
		return nil
	}
	payload := map[string]any{"spec": map[string]any{"persistentVolumeReclaimPolicy": policy}}
	data, _ := json.Marshal(payload)
	_, err = r.client.CoreV1().PersistentVolumes().Patch(ctx, pvName, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}
