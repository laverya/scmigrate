package scmigrate

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type Runner struct {
	opts      Options
	client    kubernetes.Interface
	out       io.Writer
	namespace string
}

func NewRunner(opts Options, out io.Writer) (*Runner, error) {
	if opts.TargetStorageClass == "" {
		return nil, errors.New("--target-storage-class is required")
	}
	if opts.AllNamespaces && opts.Namespace != "" {
		return nil, errors.New("use either --namespace or --all-namespaces, not both")
	}
	client, currentNamespace, err := kubernetesClient()
	if err != nil {
		return nil, err
	}
	if opts.Namespace == "" && !opts.AllNamespaces {
		opts.Namespace = currentNamespace
	}
	if opts.RunnerImage == "" {
		opts.RunnerImage = "ghcr.io/laverya/scmigrate-rsync:latest"
	}
	if opts.RsyncArgs == "" {
		opts.RsyncArgs = "-aHAX --numeric-ids --delete --info=progress2"
	}
	return &Runner{opts: opts, client: client, out: out, namespace: currentNamespace}, nil
}

func (r *Runner) Plan(ctx context.Context) error {
	migrations, err := r.discover(ctx)
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		fmt.Fprintln(r.out, "No matching PVCs found.")
		return nil
	}
	for _, migration := range migrations {
		fmt.Fprintf(r.out, "%s/%s: %s -> %s state=%s sourcePV=%s\n",
			migration.Source.Namespace,
			migration.Source.Name,
			storageClass(migration.Source),
			r.opts.TargetStorageClass,
			migration.State,
			migration.Source.Spec.VolumeName,
		)
		consumers := migration.Consumers
		fmt.Fprintf(r.out, "  consumers: %d\n", len(consumers))
		for _, consumer := range consumers {
			fmt.Fprintf(r.out, "  - pod/%s phase=%s owner=%s/%s\n",
				consumer.Pod.Name,
				consumer.Pod.Status.Phase,
				strings.ToLower(consumer.Workload.Kind),
				consumer.Workload.Name,
			)
		}
		if migration.Destination != nil {
			fmt.Fprintf(r.out, "  destination: pvc/%s pv=%s\n", migration.Destination.Name, migration.Destination.Spec.VolumeName)
		}
	}
	return nil
}

func (r *Runner) Run(ctx context.Context) error {
	migrations, err := r.discover(ctx)
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		fmt.Fprintln(r.out, "No matching PVCs found.")
		return nil
	}
	if !r.opts.Yes && !r.opts.DryRun {
		fmt.Fprintf(r.out, "About to migrate %d PVC(s) to storageClass %q. Continue [y/N]? ", len(migrations), r.opts.TargetStorageClass)
		answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if strings.TrimSpace(strings.ToLower(answer)) != "y" && strings.TrimSpace(strings.ToLower(answer)) != "yes" {
			return errors.New("aborted")
		}
	}

	for _, migration := range migrations {
		fmt.Fprintf(r.out, "Migrating %s/%s\n", migration.Source.Namespace, migration.Source.Name)
		if err := r.migrate(ctx, migration); err != nil {
			return fmt.Errorf("%s/%s: %w", migration.Source.Namespace, migration.Source.Name, err)
		}
	}
	return nil
}

func (r *Runner) migrate(ctx context.Context, migration *Migration) error {
	if r.opts.DryRun {
		return r.dryRunMigrate(ctx, migration)
	}
	source := migration.Source
	source, state, err := r.currentPVCState(ctx, source.Namespace, source.Name)
	if apierrors.IsNotFound(err) {
		if err := r.cutover(ctx, migration.Source); err != nil {
			return err
		}
		source, state, err = r.currentPVCState(ctx, migration.Source.Namespace, migration.Source.Name)
	}
	if err != nil {
		return err
	}
	if stateBefore(state, StatePrepared) {
		if err := r.prepare(ctx, source); err != nil {
			return err
		}
		source, state, err = r.currentPVCState(ctx, source.Namespace, source.Name)
		if err != nil {
			return err
		}
	}
	if !r.opts.SkipInitialSync && stateBefore(state, StateInitialSynced) {
		if err := r.runSync(ctx, source, "initial"); err != nil {
			return err
		}
		if err := r.patchPVCAnnotations(ctx, source, map[string]string{AnnState: StateInitialSynced}); err != nil {
			return err
		}
		source, state, err = r.currentPVCState(ctx, source.Namespace, source.Name)
		if err != nil {
			return err
		}
	}
	if stateBefore(state, StateQuiesced) {
		if err := r.quiesce(ctx, source); err != nil {
			return err
		}
		if err := r.patchPVCAnnotations(ctx, source, map[string]string{AnnState: StateQuiesced}); err != nil {
			return err
		}
		source, state, err = r.currentPVCState(ctx, source.Namespace, source.Name)
		if err != nil {
			return err
		}
	}
	if stateBefore(state, StateFinalSynced) {
		if err := r.runSync(ctx, source, "final"); err != nil {
			return err
		}
		if err := r.patchPVCAnnotations(ctx, source, map[string]string{AnnState: StateFinalSynced}); err != nil {
			return err
		}
		source, state, err = r.currentPVCState(ctx, source.Namespace, source.Name)
		if err != nil {
			return err
		}
	}
	if stateBefore(state, StateCutover) {
		if err := r.cutover(ctx, source); err != nil {
			return err
		}
		source, state, err = r.currentPVCState(ctx, source.Namespace, source.Name)
		if err != nil {
			return err
		}
	}
	if stateBefore(state, StateRestored) {
		if err := r.restoreWorkload(ctx, source); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) dryRunMigrate(ctx context.Context, migration *Migration) error {
	source := migration.Source.DeepCopy()
	current, state, err := r.currentPVCState(ctx, source.Namespace, source.Name)
	if err == nil {
		source = current.DeepCopy()
	} else if apierrors.IsNotFound(err) {
		state = migration.State
		if state == "" {
			state = StateFinalSynced
		}
	} else {
		return err
	}
	if err := r.validateTargetStorageClass(source.Namespace+"/"+source.Name, source.Annotations); err != nil {
		return err
	}
	if source.Annotations == nil {
		source.Annotations = map[string]string{}
	}
	if source.Annotations[AnnDestinationPVC] == "" {
		source.Annotations[AnnDestinationPVC] = tempPVCName(source)
	}
	if stateBefore(state, StatePrepared) {
		if err := validateSourcePVC(source); err != nil {
			return err
		}
		fmt.Fprintf(r.out, "dry-run: patch pvc/%s annotations %v\n", source.Name, []string{AnnDestinationPVC, AnnOriginalPVC, AnnSourcePV, AnnState, AnnTargetStorageClass})
		fmt.Fprintf(r.out, "dry-run: create destination pvc/%s in %s\n", source.Annotations[AnnDestinationPVC], source.Namespace)
		state = StatePrepared
	}
	if !r.opts.SkipInitialSync && stateBefore(state, StateInitialSynced) {
		fmt.Fprintf(r.out, "dry-run: run initial sync pod/%s from pvc/%s to pvc/%s\n", syncPodName(source, "initial"), source.Name, source.Annotations[AnnDestinationPVC])
		state = StateInitialSynced
	}
	if stateBefore(state, StateQuiesced) {
		records, err := r.dryRunQuiesce(ctx, source)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(records)
		if err != nil {
			return err
		}
		source.Annotations[AnnQuiesce] = string(raw)
		state = StateQuiesced
	}
	if stateBefore(state, StateFinalSynced) {
		fmt.Fprintf(r.out, "dry-run: run final sync pod/%s from pvc/%s to pvc/%s\n", syncPodName(source, "final"), source.Name, source.Annotations[AnnDestinationPVC])
		state = StateFinalSynced
	}
	if stateBefore(state, StateCutover) {
		if err := r.dryRunCutover(ctx, source); err != nil {
			return err
		}
		state = StateCutover
	}
	if stateBefore(state, StateRestored) {
		fmt.Fprintf(r.out, "dry-run: restore workload(s) and mark pvc/%s state=%s\n", source.Name, StateRestored)
	}
	return nil
}

func (r *Runner) dryRunQuiesce(ctx context.Context, pvc *corev1.PersistentVolumeClaim) ([]QuiesceRecord, error) {
	var records []QuiesceRecord
	if raw := pvc.Annotations[AnnQuiesce]; raw != "" {
		parsed, err := quiesceRecordsFromAnnotation(raw)
		if err != nil {
			return nil, err
		}
		records = parsed
	} else {
		consumers, err := r.pvcConsumers(ctx, pvc)
		if err != nil {
			return nil, err
		}
		parsed, err := r.quiesceConsumerGroups(ctx, pvc, consumers)
		if err != nil {
			return nil, err
		}
		records = parsed
	}
	fmt.Fprintf(r.out, "dry-run: store %d quiesce record(s) on pvc/%s and destination pvc/%s\n", len(records), pvc.Name, pvc.Annotations[AnnDestinationPVC])
	return records, r.applyQuiesceRecords(ctx, records)
}

func (r *Runner) dryRunCutover(ctx context.Context, source *corev1.PersistentVolumeClaim) error {
	destName := source.Annotations[AnnDestinationPVC]
	destPV := source.Annotations[AnnDestinationPV]
	if dest, err := r.destinationPVC(ctx, source); err == nil {
		destName = dest.Name
		destPV = dest.Spec.VolumeName
		if err := r.validateTargetStorageClass("pvc/"+dest.Namespace+"/"+dest.Name, dest.Annotations); err != nil {
			return err
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	if destPV == "" {
		destPV = "<destination-pv-after-binding>"
	}
	fmt.Fprintf(r.out, "dry-run: wait for destination pvc/%s to bind\n", destName)
	if source.Spec.VolumeName != "" {
		fmt.Fprintf(r.out, "dry-run: set pv/%s reclaimPolicy=Retain\n", source.Spec.VolumeName)
	}
	fmt.Fprintf(r.out, "dry-run: set pv/%s reclaimPolicy=Retain\n", destPV)
	fmt.Fprintf(r.out, "dry-run: persist cutover record on pv/%s\n", destPV)
	fmt.Fprintf(r.out, "dry-run: delete pvc/%s and pvc/%s, clear pv/%s claimRef, create final pvc/%s bound to pv/%s\n", source.Name, destName, destPV, source.Name, destPV)
	return nil
}

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
			migration := &Migration{Source: pvc, SourcePV: pv, State: pvc.Annotations[AnnState]}
			if migration.State == "" {
				migration.State = "new"
			}
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
		LabelSelector: fmt.Sprintf("%s=scmigrate,%s=destination", LabelManagedBy, LabelRole),
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
	if pvc.Labels[LabelManagedBy] == "scmigrate" {
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
	if state == "" {
		state = "new"
	}
	return pvc, state, nil
}

func (r *Runner) prepare(ctx context.Context, source *corev1.PersistentVolumeClaim) error {
	if err := validateSourcePVC(source); err != nil {
		return err
	}
	tempName := tempPVCName(source)
	snapshot, err := pvcSnapshot(source)
	if err != nil {
		return err
	}
	nextState := source.Annotations[AnnState]
	if stateBefore(nextState, StatePrepared) {
		nextState = StatePrepared
	}
	if err := r.patchPVCAnnotations(ctx, source, map[string]string{
		AnnState:              nextState,
		AnnDestinationPVC:     tempName,
		AnnSourcePV:           source.Spec.VolumeName,
		AnnTargetStorageClass: r.opts.TargetStorageClass,
		AnnOriginalPVC:        snapshot,
	}); err != nil {
		return err
	}

	if _, err := r.destinationPVC(ctx, source); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	request := source.Spec.Resources.Requests[corev1.ResourceStorage]
	dest := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tempName,
			Namespace: source.Namespace,
			Labels: map[string]string{
				LabelManagedBy: "scmigrate",
				LabelRole:      "destination",
				LabelSourceUID: string(source.UID),
			},
			Annotations: map[string]string{
				AnnState:              StatePrepared,
				AnnSourcePVC:          source.Name,
				AnnSourceNamespace:    source.Namespace,
				AnnSourceUID:          string(source.UID),
				AnnSourcePV:           source.Spec.VolumeName,
				AnnTargetStorageClass: r.opts.TargetStorageClass,
				AnnOriginalPVC:        snapshot,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      source.Spec.AccessModes,
			VolumeMode:       source.Spec.VolumeMode,
			StorageClassName: &r.opts.TargetStorageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: request},
			},
		},
	}
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: create destination pvc/%s in %s\n", dest.Name, dest.Namespace)
		return nil
	}
	created, err := r.client.CoreV1().PersistentVolumeClaims(source.Namespace).Create(ctx, dest, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	if err == nil {
		fmt.Fprintf(r.out, "created destination pvc/%s\n", created.Name)
	}
	return nil
}

func (r *Runner) runSync(ctx context.Context, source *corev1.PersistentVolumeClaim, phase string) error {
	dest, err := r.destinationPVC(ctx, source)
	if err != nil {
		return err
	}
	podName := syncPodName(source, phase)
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: run %s sync pod/%s from pvc/%s to pvc/%s\n", phase, podName, source.Name, dest.Name)
		return nil
	}
	if old, err := r.client.CoreV1().Pods(source.Namespace).Get(ctx, podName, metav1.GetOptions{}); err == nil {
		if old.Status.Phase == corev1.PodSucceeded {
			fmt.Fprintf(r.out, "%s sync already completed by pod/%s\n", phase, podName)
			return r.cleanupSyncPod(ctx, source.Namespace, podName)
		}
		if old.Status.Phase == corev1.PodFailed {
			_ = r.client.CoreV1().Pods(source.Namespace).Delete(ctx, podName, metav1.DeleteOptions{})
			if err := r.waitPodGone(ctx, source.Namespace, podName); err != nil {
				return err
			}
		} else {
			if err := r.waitPodSucceeded(ctx, source.Namespace, podName); err != nil {
				return err
			}
			return r.cleanupSyncPod(ctx, source.Namespace, podName)
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	args := []string{"sh", "-c", fmt.Sprintf("rsync %s /source/ /destination/", r.opts.RsyncArgs)}
	nodeName := ""
	if phase == "initial" {
		nodeName, err = r.syncNodeName(ctx, source)
		if err != nil {
			return err
		}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: source.Namespace,
			Labels: map[string]string{
				LabelManagedBy: "scmigrate",
				LabelRole:      "sync",
				LabelSourceUID: string(source.UID),
			},
			Annotations: map[string]string{
				AnnSourcePVC:          source.Name,
				AnnDestinationPVC:     dest.Name,
				AnnTargetStorageClass: r.opts.TargetStorageClass,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            "rsync",
				Image:           r.opts.RunnerImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         args[:2],
				Args:            args[2:],
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:  ptrInt64(0),
					RunAsGroup: ptrInt64(0),
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "source", MountPath: "/source", ReadOnly: true},
					{Name: "destination", MountPath: "/destination"},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "source", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: source.Name, ReadOnly: true}}},
				{Name: "destination", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: dest.Name}}},
			},
		},
	}
	if nodeName != "" {
		pod.Spec.Affinity = affinityForNode(nodeName)
	}
	if _, err := r.client.CoreV1().Pods(source.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return err
	}
	fmt.Fprintf(r.out, "started %s sync pod/%s\n", phase, podName)
	if err := r.waitPodSucceeded(ctx, source.Namespace, podName); err != nil {
		return err
	}
	return r.cleanupSyncPod(ctx, source.Namespace, podName)
}

func (r *Runner) cleanupSyncPod(ctx context.Context, namespace, name string) error {
	err := r.client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.waitPodGone(ctx, namespace, name)
}

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
	stored, err := storedPVC(dest.Annotations[AnnOriginalPVC])
	if err != nil {
		return err
	}
	finalPVC, err := finalPVCFromStored(stored, r.opts.TargetStorageClass, dest.Spec.VolumeName)
	if err != nil {
		return err
	}
	if source.Annotations[AnnQuiesce] != "" {
		finalPVC.Annotations[AnnQuiesce] = source.Annotations[AnnQuiesce]
	}
	finalPVC.Annotations[AnnDestinationPVC] = dest.Name
	finalPVC.Annotations[AnnOriginalPVC] = dest.Annotations[AnnOriginalPVC]
	if _, err := r.client.CoreV1().PersistentVolumeClaims(finalPVC.Namespace).Create(ctx, finalPVC, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	if err := r.waitPVCBound(ctx, finalPVC.Namespace, finalPVC.Name); err != nil {
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
		LabelSelector: fmt.Sprintf("%s=scmigrate,%s=destination", LabelManagedBy, LabelRole),
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
	stored, err := storedPVC(dest.Annotations[AnnOriginalPVC])
	if err != nil {
		return err
	}
	finalPVC, err := finalPVCFromStored(stored, r.opts.TargetStorageClass, dest.Spec.VolumeName)
	if err != nil {
		return err
	}
	if dest.Annotations[AnnQuiesce] != "" {
		finalPVC.Annotations[AnnQuiesce] = dest.Annotations[AnnQuiesce]
	}
	finalPVC.Annotations[AnnDestinationPVC] = dest.Name
	finalPVC.Annotations[AnnOriginalPVC] = dest.Annotations[AnnOriginalPVC]
	_, err = r.client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, finalPVC, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return r.waitPVCBound(ctx, namespace, finalPVC.Name)
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
		stored, err := storedPVC(annotations[AnnOriginalPVC])
		if err != nil {
			return err
		}
		finalPVC, err := finalPVCFromStored(stored, r.opts.TargetStorageClass, pv.Name)
		if err != nil {
			return err
		}
		if annotations[AnnQuiesce] != "" {
			finalPVC.Annotations[AnnQuiesce] = annotations[AnnQuiesce]
		}
		finalPVC.Annotations[AnnDestinationPVC] = annotations[AnnDestinationPVC]
		finalPVC.Annotations[AnnOriginalPVC] = annotations[AnnOriginalPVC]
		_, err = r.client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, finalPVC, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		if err := r.waitPVCBound(ctx, namespace, finalPVC.Name); err != nil {
			return err
		}
		if r.opts.RestoreReclaimPolicy {
			return r.restoreDestinationReclaimPolicy(ctx, pv.Name)
		}
		return nil
	}
	return fmt.Errorf("source PVC is gone and no resumable destination PVC or PV cutover record for %s/%s was found", namespace, name)
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

func (r *Runner) patchPVCAnnotations(ctx context.Context, pvc *corev1.PersistentVolumeClaim, annotations map[string]string) error {
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: patch pvc/%s annotations %v\n", pvc.Name, sortedKeys(annotations))
		return nil
	}
	payload := map[string]any{"metadata": map[string]any{"annotations": annotations}}
	data, _ := json.Marshal(payload)
	_, err := r.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(ctx, pvc.Name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
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

func (r *Runner) waitPVCBound(ctx context.Context, namespace, name string) error {
	return wait(ctx, 20*time.Minute, func() (bool, error) {
		pvc, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "", nil
	})
}

func (r *Runner) waitPVCGone(ctx context.Context, namespace, name string) error {
	return wait(ctx, 10*time.Minute, func() (bool, error) {
		_, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func (r *Runner) waitPodGone(ctx context.Context, namespace, name string) error {
	return wait(ctx, 10*time.Minute, func() (bool, error) {
		_, err := r.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func (r *Runner) waitPodSucceeded(ctx context.Context, namespace, name string) error {
	return wait(ctx, 24*time.Hour, func() (bool, error) {
		pod, err := r.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("pod/%s failed", name)
		}
		return pod.Status.Phase == corev1.PodSucceeded, nil
	})
}

func (r *Runner) consumingPods(ctx context.Context, pvc *corev1.PersistentVolumeClaim) ([]corev1.Pod, error) {
	pods, err := r.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("status.phase", string(corev1.PodRunning)).String(),
	})
	if err != nil {
		return nil, err
	}
	var consumers []corev1.Pod
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
				consumers = append(consumers, pod)
				break
			}
		}
	}
	return consumers, nil
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
		return WorkloadRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, nil
	}

	switch owner.Kind {
	case "ReplicaSet":
		rs, err := r.client.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			return WorkloadRef{}, err
		}
		if deployOwner := controllerOwner(rs.OwnerReferences); deployOwner != nil && deployOwner.Kind == "Deployment" {
			return WorkloadRef{Kind: "Deployment", Namespace: pod.Namespace, Name: deployOwner.Name}, nil
		}
		return WorkloadRef{Kind: "ReplicaSet", Namespace: pod.Namespace, Name: owner.Name}, nil
	case "StatefulSet", "DaemonSet":
		return WorkloadRef{Kind: owner.Kind, Namespace: pod.Namespace, Name: owner.Name}, nil
	default:
		return WorkloadRef{}, fmt.Errorf("unsupported pod controller %s/%s for pod/%s", owner.Kind, owner.Name, pod.Name)
	}
}

func (r *Runner) syncNodeName(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, error) {
	consumers, err := r.consumingPods(ctx, pvc)
	if err != nil {
		return "", err
	}
	if len(consumers) == 0 {
		return "", nil
	}
	nodeName := consumers[0].Spec.NodeName
	if nodeName == "" {
		return "", nil
	}
	for _, consumer := range consumers[1:] {
		if consumer.Spec.NodeName != nodeName {
			return "", nil
		}
	}
	return nodeName, nil
}

func affinityForNode(nodeName string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchFields: []corev1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{nodeName},
					}},
				}},
			},
		},
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

func stateBefore(current, target string) bool {
	order := map[string]int{
		"":                 0,
		"new":              0,
		StatePrepared:      1,
		StateInitialSynced: 2,
		StateQuiesced:      3,
		StateFinalSynced:   4,
		StateCutover:       5,
		StateRestored:      6,
	}
	return order[current] < order[target]
}

func ptrInt64(value int64) *int64 {
	return &value
}
