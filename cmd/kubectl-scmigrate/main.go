package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/laverya/scmigrate/pkg/scmigrate"
	buildversion "github.com/laverya/scmigrate/pkg/version"
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

	command := os.Args[1]
	if command == "help" || command == "--help" || command == "-h" {
		usage()
		return
	}
	if command == "version" {
		buildversion.Print(os.Stdout)
		return
	}

	var annotations annotationFilters
	fs := flag.NewFlagSet("kubectl-scmigrate", flag.ExitOnError)
	opts := scmigrate.Options{RunnerImage: buildversion.DefaultRunnerImage(buildversion.Effective())}
	fs.StringVar(&opts.Namespace, "namespace", "", "Namespace to scan. Defaults to the current kubeconfig namespace.")
	fs.BoolVar(&opts.AllNamespaces, "all-namespaces", false, "Scan all namespaces.")
	fs.StringVar(&opts.Selector, "selector", "", "PVC label selector.")
	fs.Var(&annotations, "annotation", "PVC annotation filter in key=value form. Repeatable.")
	fs.StringVar(&opts.SourceStorageClass, "source-storage-class", "", "Only migrate PVCs currently using this storageClass.")
	fs.StringVar(&opts.TargetStorageClass, "target-storage-class", "", "Destination storageClass.")
	fs.StringVar(&opts.RunnerImage, "runner-image", opts.RunnerImage, "Image used by rsync pods. Required for run unless this binary has a release-version default; use a non-latest tag or digest.")
	fs.StringVar(&opts.RsyncArgs, "rsync-args", scmigrate.DefaultRsyncArgs, "Whitespace-separated arguments passed to rsync.")
	fs.BoolVar(&opts.Yes, "yes", false, "Apply changes without prompting.")
	fs.BoolVar(&opts.DryRun, "dry-run", false, "Show actions without changing the cluster.")
	fs.BoolVar(&opts.SkipInitialSync, "skip-initial-sync", false, "Skip the live initial rsync phase.")
	fs.BoolVar(&opts.RestoreReclaimPolicy, "restore-reclaim-policy", false, "Restore the destination PV reclaim policy after cutover.")

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
  kubectl scmigrate version

Selection flags:
  --namespace ns                  Scan one namespace
  --all-namespaces                Scan every namespace
  --selector app=db,tier=primary  Match PVC labels
  --annotation key=value          Match PVC annotations; repeatable
  --source-storage-class slow     Restrict source storageClass

Safety flags:
  --dry-run                       Print changes only

Runner flags:
  --runner-image image:tag        Required for run unless release default is available; latest is rejected
  --rsync-args args               Whitespace-separated rsync arguments
`)
}
