package scmigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

func (r *Runner) discover(ctx context.Context) ([]*Migration, error) {
	namespaces := []string{r.opts.Namespace}
	if r.opts.AllNamespaces {
		list, err := r.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		namespaces = nil
		for _, ns := range list.Items {
			namespaces = append(namespaces, ns.Name)
		}
	}

	var migrations []*Migration
	for _, namespace := range namespaces {
		pvcs, err := r.client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{LabelSelector: r.opts.Selector})
		if err != nil {
			return nil, err
		}
		for i := range pvcs.Items {
			pvc := pvcs.Items[i].DeepCopy()
			if r.skipPVC(pvc) {
				continue
			}
			if err := validateSourcePVC(pvc); err != nil {
				return nil, fmt.Errorf("%s/%s: %w", pvc.Namespace, pvc.Name, err)
			}
			if err := r.validateTargetStorageClass("pvc/"+pvc.Namespace+"/"+pvc.Name, pvc.Annotations); err != nil {
				return nil, err
			}
			pv, err := r.boundPV(ctx, pvc)
			if err != nil {
				return nil, err
			}
			state, err := normalizeMigrationState(pvc.Annotations[AnnState])
			if err != nil {
				return nil, fmt.Errorf("%s/%s: %w", pvc.Namespace, pvc.Name, err)
			}
			migration := &Migration{Source: pvc, SourcePV: pv, State: state}
			if dest, err := r.destinationPVC(ctx, pvc); err == nil {
				if err := r.validateTargetStorageClass("pvc/"+dest.Namespace+"/"+dest.Name, dest.Annotations); err != nil {
					return nil, err
				}
				migration.Destination = dest
				if dest.Spec.VolumeName != "" {
					if destPV, pvErr := r.client.CoreV1().PersistentVolumes().Get(ctx, dest.Spec.VolumeName, metav1.GetOptions{}); pvErr == nil {
						migration.DestPV = destPV
					}
				}
			} else if !apierrors.IsNotFound(err) {
				return nil, err
			}
			consumers, err := r.pvcConsumers(ctx, pvc)
			if err != nil {
				return nil, err
			}
			migration.Consumers = consumers
			migrations = append(migrations, migration)
		}
		resumable, err := r.discoverResumableDestinations(ctx, namespace)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, resumable...)
		pvResumable, err := r.discoverResumableCutoverPVs(ctx, namespace)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, pvResumable...)
	}
	sortMigrations(migrations)
	return migrations, nil
}

func (r *Runner) discoverResumableDestinations(ctx context.Context, namespace string) ([]*Migration, error) {
	destinations, err := r.client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=destination", LabelManagedBy, ManagedByValue, LabelRole),
	})
	if err != nil {
		return nil, err
	}
	var migrations []*Migration
	for i := range destinations.Items {
		dest := destinations.Items[i].DeepCopy()
		sourceName := dest.Annotations[AnnSourcePVC]
		if sourceName == "" {
			continue
		}
		if err := r.validateTargetStorageClass("pvc/"+dest.Namespace+"/"+dest.Name, dest.Annotations); err != nil {
			return nil, err
		}
		if _, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, sourceName, metav1.GetOptions{}); err == nil {
			continue
		} else if !apierrors.IsNotFound(err) {
			return nil, err
		}
		stored, err := storedPVC(dest.Annotations[AnnOriginalPVC])
		if err != nil {
			return nil, err
		}
		matches, err := r.storedPVCMatchesOptions(stored)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		if err := validateStoredPVC(stored); err != nil {
			return nil, fmt.Errorf("%s/%s: %w", stored.Namespace, stored.Name, err)
		}
		state := dest.Annotations[AnnState]
		if state == "" {
			state = StateFinalSynced
		} else if state, err = normalizeMigrationState(state); err != nil {
			return nil, fmt.Errorf("%s/%s: %w", dest.Namespace, dest.Name, err)
		}
		source := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        sourceName,
				Namespace:   namespace,
				UID:         types.UID(dest.Annotations[AnnSourceUID]),
				Labels:      cloneMap(stored.Labels),
				Annotations: map[string]string{AnnDestinationPVC: dest.Name, AnnState: state},
			},
			Spec: corev1.PersistentVolumeClaimSpec{VolumeName: dest.Annotations[AnnSourcePV]},
		}
		migration := &Migration{Source: source, Destination: dest, State: state}
		if dest.Spec.VolumeName != "" {
			if destPV, err := r.client.CoreV1().PersistentVolumes().Get(ctx, dest.Spec.VolumeName, metav1.GetOptions{}); err == nil {
				migration.DestPV = destPV
			} else if !apierrors.IsNotFound(err) {
				return nil, err
			}
		}
		migrations = append(migrations, migration)
	}
	return migrations, nil
}

func (r *Runner) discoverResumableCutoverPVs(ctx context.Context, namespace string) ([]*Migration, error) {
	pvs, err := r.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var migrations []*Migration
	for i := range pvs.Items {
		pv := pvs.Items[i].DeepCopy()
		annotations := pv.Annotations
		if annotations[AnnSourceNamespace] != namespace || annotations[AnnOriginalPVC] == "" {
			continue
		}
		sourceName := annotations[AnnSourcePVC]
		if sourceName == "" {
			continue
		}
		if err := r.validateTargetStorageClass("pv/"+pv.Name, annotations); err != nil {
			return nil, err
		}
		if _, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, sourceName, metav1.GetOptions{}); err == nil {
			continue
		} else if !apierrors.IsNotFound(err) {
			return nil, err
		}
		if destName := annotations[AnnDestinationPVC]; destName != "" {
			if _, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, destName, metav1.GetOptions{}); err == nil {
				continue
			} else if !apierrors.IsNotFound(err) {
				return nil, err
			}
		}
		stored, err := storedPVC(annotations[AnnOriginalPVC])
		if err != nil {
			return nil, err
		}
		matches, err := r.storedPVCMatchesOptions(stored)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		if err := validateStoredPVC(stored); err != nil {
			return nil, fmt.Errorf("%s/%s: %w", stored.Namespace, stored.Name, err)
		}
		source := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sourceName,
				Namespace: namespace,
				UID:       types.UID(annotations[AnnSourceUID]),
				Labels:    cloneMap(stored.Labels),
				Annotations: map[string]string{
					AnnDestinationPVC:     annotations[AnnDestinationPVC],
					AnnDestinationPV:      pv.Name,
					AnnOriginalPVC:        annotations[AnnOriginalPVC],
					AnnQuiesce:            annotations[AnnQuiesce],
					AnnState:              StateFinalSynced,
					AnnTargetStorageClass: annotations[AnnTargetStorageClass],
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{VolumeName: annotations[AnnSourcePV]},
		}
		migrations = append(migrations, &Migration{Source: source, DestPV: pv, State: StateFinalSynced})
	}
	return migrations, nil
}

func (r *Runner) storedPVCMatchesOptions(stored *StoredPVC) (bool, error) {
	if r.opts.Selector != "" {
		selector, err := labels.Parse(r.opts.Selector)
		if err != nil {
			return false, err
		}
		if !selector.Matches(labels.Set(stored.Labels)) {
			return false, nil
		}
	}
	sourceSC := ""
	if stored.StorageClassName != nil {
		sourceSC = *stored.StorageClassName
	}
	if r.opts.SourceStorageClass != "" && sourceSC != r.opts.SourceStorageClass {
		return false, nil
	}
	for _, filter := range r.opts.AnnotationFilters {
		key, value, ok := strings.Cut(filter, "=")
		if !ok || key == "" {
			return false, nil
		}
		if stored.Annotations[key] != value {
			return false, nil
		}
	}
	return true, nil
}

func (r *Runner) skipPVC(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return true
	}
	if pvc.Labels[LabelManagedBy] == ManagedByValue {
		return true
	}
	if r.opts.SourceStorageClass != "" && storageClass(pvc) != r.opts.SourceStorageClass {
		return true
	}
	if storageClass(pvc) == r.opts.TargetStorageClass && pvc.Annotations[AnnState] == "" {
		return true
	}
	for _, filter := range r.opts.AnnotationFilters {
		key, value, ok := strings.Cut(filter, "=")
		if !ok || key == "" {
			return true
		}
		if pvc.Annotations[key] != value {
			return true
		}
	}
	return false
}

func validateSourcePVC(pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		return errors.New("block volumeMode is not supported; scmigrate requires filesystem PVCs")
	}
	for _, mode := range pvc.Spec.AccessModes {
		if mode == corev1.ReadWriteOncePod {
			return errors.New("ReadWriteOncePod access mode is not supported because sync pods need to mount the PVC during migration")
		}
	}
	return nil
}

func validateStoredPVC(stored *StoredPVC) error {
	if stored.VolumeMode != nil && *stored.VolumeMode == corev1.PersistentVolumeBlock {
		return errors.New("block volumeMode is not supported; scmigrate requires filesystem PVCs")
	}
	for _, mode := range stored.AccessModes {
		if mode == corev1.ReadWriteOncePod {
			return errors.New("ReadWriteOncePod access mode is not supported because sync pods need to mount the PVC during migration")
		}
	}
	return nil
}

func (r *Runner) validateTargetStorageClass(object string, annotations map[string]string) error {
	recorded := annotations[AnnTargetStorageClass]
	if recorded == "" || recorded == r.opts.TargetStorageClass {
		return nil
	}
	return fmt.Errorf("%s was prepared for target storageClass %q; rerun with --target-storage-class %s instead of %q", object, recorded, recorded, r.opts.TargetStorageClass)
}

func (r *Runner) currentPVCState(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, string, error) {
	pvc, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, "", err
	}
	state := pvc.Annotations[AnnState]
	state, err = normalizeMigrationState(state)
	if err != nil {
		return nil, "", err
	}
	return pvc, state, nil
}

func (r *Runner) boundPV(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolume, error) {
	return r.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
}

func (r *Runner) destinationPVC(ctx context.Context, source *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	name := source.Annotations[AnnDestinationPVC]
	if name == "" {
		name = tempPVCName(source)
	}
	return r.client.CoreV1().PersistentVolumeClaims(source.Namespace).Get(ctx, name, metav1.GetOptions{})
}
