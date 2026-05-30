package scmigrate

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (r *Runner) migrate(ctx context.Context, migration *Migration) error {
	if r.opts.DryRun {
		return r.dryRunMigrate(ctx, migration)
	}
	// Each durable phase records enough cluster state for the next invocation to
	// resume from Kubernetes objects instead of trusting the original Migration.
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
	before, err := stateBefore(state, StatePrepared)
	if err != nil {
		return err
	}
	if before {
		if err := r.prepare(ctx, source); err != nil {
			return err
		}
		source, state, err = r.currentPVCState(ctx, source.Namespace, source.Name)
		if err != nil {
			return err
		}
	}
	before, err = stateBefore(state, StateInitialSynced)
	if err != nil {
		return err
	}
	if !r.opts.SkipInitialSync && before {
		if err := r.runSync(ctx, source, SyncPhaseInitial); err != nil {
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
	before, err = stateBefore(state, StateQuiesced)
	if err != nil {
		return err
	}
	if before {
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
	before, err = stateBefore(state, StateFinalSynced)
	if err != nil {
		return err
	}
	if before {
		if err := r.runSync(ctx, source, SyncPhaseFinal); err != nil {
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
	before, err = stateBefore(state, StateCutover)
	if err != nil {
		return err
	}
	if before {
		if err := r.cutover(ctx, source); err != nil {
			return err
		}
		source, state, err = r.currentPVCState(ctx, source.Namespace, source.Name)
		if err != nil {
			return err
		}
	}
	before, err = stateBefore(state, StateRestored)
	if err != nil {
		return err
	}
	if before {
		if err := r.restoreWorkload(ctx, source); err != nil {
			return err
		}
	}
	return nil
}
