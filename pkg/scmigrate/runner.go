package scmigrate

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"k8s.io/client-go/kubernetes"
)

type Runner struct {
	opts   Options
	client kubernetes.Interface
	out    io.Writer
}

func NewRunner(opts Options, out io.Writer) (*Runner, error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, errors.New("output writer is required")
	}
	client, currentNamespace, err := kubernetesClient()
	if err != nil {
		return nil, err
	}
	if opts.Namespace == "" && !opts.AllNamespaces {
		opts.Namespace = currentNamespace
	}
	if opts.RcloneArgs == "" {
		opts.RcloneArgs = DefaultRcloneArgs
	}
	return &Runner{opts: opts, client: client, out: out}, nil
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
	if !r.opts.DryRun {
		if err := validateRunnerImage(r.opts.RunnerImage); err != nil {
			return err
		}
	}
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
		answer, readErr := bufio.NewReader(os.Stdin).ReadString('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("read confirmation: %w", readErr)
		}
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
