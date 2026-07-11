package scmigrate

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

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
	before, err := stateBefore(state, StatePrepared)
	if err != nil {
		return err
	}
	if before {
		if err := validateSourcePVC(source); err != nil {
			return err
		}
		fmt.Fprintf(r.out, "dry-run: patch pvc/%s annotations %v\n", source.Name, []string{AnnDestinationPVC, AnnOriginalPVC, AnnSourcePV, AnnState, AnnTargetStorageClass})
		fmt.Fprintf(r.out, "dry-run: create destination pvc/%s in %s\n", source.Annotations[AnnDestinationPVC], source.Namespace)
		state = StatePrepared
	}
	before, err = stateBefore(state, StateInitialSynced)
	if err != nil {
		return err
	}
	if !r.opts.SkipInitialSync && before {
		fmt.Fprintf(r.out, "dry-run: run initial sync pod/%s from pvc/%s to pvc/%s\n", syncPodName(source, SyncPhaseInitial), source.Name, source.Annotations[AnnDestinationPVC])
		state = StateInitialSynced
	}
	before, err = stateBefore(state, StateQuiesced)
	if err != nil {
		return err
	}
	if before {
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
	before, err = stateBefore(state, StateFinalSynced)
	if err != nil {
		return err
	}
	if before {
		fmt.Fprintf(r.out, "dry-run: run final sync pod/%s from pvc/%s to pvc/%s\n", syncPodName(source, SyncPhaseFinal), source.Name, source.Annotations[AnnDestinationPVC])
		state = StateFinalSynced
	}
	before, err = stateBefore(state, StateCutover)
	if err != nil {
		return err
	}
	if before {
		if err := r.dryRunCutover(ctx, source); err != nil {
			return err
		}
		state = StateCutover
	}
	before, err = stateBefore(state, StateRestored)
	if err != nil {
		return err
	}
	if before {
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
		if err := validateQuiesceRecords(parsed, pvc.Namespace); err != nil {
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
