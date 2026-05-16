package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/laverya/scmigrate/internal/scmigrate"
)

type annotationFilters []string

func (a *annotationFilters) String() string {
	return strings.Join(*a, ",")
}

func (a *annotationFilters) Set(value string) error {
	*a = append(*a, value)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var annotations annotationFilters
	fs := flag.NewFlagSet("kubectl-scmigrate", flag.ExitOnError)
	opts := scmigrate.Options{}
	fs.StringVar(&opts.Namespace, "namespace", "", "Namespace to scan. Defaults to the current kubeconfig namespace.")
	fs.BoolVar(&opts.AllNamespaces, "all-namespaces", false, "Scan all namespaces.")
	fs.StringVar(&opts.Selector, "selector", "", "PVC label selector.")
	fs.Var(&annotations, "annotation", "PVC annotation filter in key=value form. Repeatable.")
	fs.StringVar(&opts.SourceStorageClass, "source-storage-class", "", "Only migrate PVCs currently using this storageClass.")
	fs.StringVar(&opts.TargetStorageClass, "target-storage-class", "", "Destination storageClass.")
	fs.StringVar(&opts.RunnerImage, "runner-image", "ghcr.io/laverya/scmigrate-rsync:latest", "Image used by rsync pods.")
	fs.StringVar(&opts.RsyncArgs, "rsync-args", "-aHAX --numeric-ids --delete --info=progress2", "Arguments passed to rsync.")
	fs.BoolVar(&opts.Yes, "yes", false, "Apply changes without prompting.")
	fs.BoolVar(&opts.DryRun, "dry-run", false, "Show actions without changing the cluster.")
	fs.BoolVar(&opts.AllowMultipleConsumers, "allow-multiple-consumers", false, "Finalize PVCs mounted by multiple pods. Use only when external quiescing is guaranteed.")
	fs.BoolVar(&opts.SkipInitialSync, "skip-initial-sync", false, "Skip the live initial rsync phase.")
	fs.BoolVar(&opts.RestoreReclaimPolicy, "restore-reclaim-policy", false, "Restore the destination PV reclaim policy after cutover.")

	command := os.Args[1]
	if command == "help" || command == "--help" || command == "-h" {
		usage()
		return
	}
	if err := fs.Parse(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	opts.AnnotationFilters = annotations

	ctx := context.Background()
	runner, err := scmigrate.NewRunner(opts, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	switch command {
	case "plan":
		err = runner.Plan(ctx)
	case "run":
		err = runner.Run(ctx)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `kubectl scmigrate migrates selected PVCs from one storageClass to another.

Usage:
  kubectl scmigrate plan --target-storage-class fast [flags]
  kubectl scmigrate run  --target-storage-class fast --yes [flags]

Selection flags:
  --namespace ns                  Scan one namespace
  --all-namespaces                Scan every namespace
  --selector app=db,tier=primary  Match PVC labels
  --annotation key=value          Match PVC annotations; repeatable
  --source-storage-class slow     Restrict source storageClass

Safety flags:
  --dry-run                       Print changes only
  --allow-multiple-consumers      Disable the default single-writer guard
`)
}
