# scmigrate Architecture and Maintainability Review

This document describes the current code layout and the safety boundaries that
matter during review. It replaces the original cleanup checklist, whose file
references and pending work items no longer matched the implementation.

## Architecture

The `pkg/scmigrate` package is organized around the migration lifecycle:

- `runner.go` owns construction and the public `Plan` and `Run` entry points.
- `validation.go` validates CLI options, filters, and supported PVC modes.
- `discovery.go` selects fresh and resumable migrations from cluster state.
- `migration.go` is the phase orchestrator and reloads durable state after each
  phase.
- `sync.go` prepares destination PVCs and runs initial/final rclone pods.
- `workload_quiesce.go`, `workload_apply.go`, and `workload_restore.go` plan,
  apply, and reverse workload-specific mutations.
- `cutover.go` owns reclaim-policy changes, durable PV recovery records, PVC
  replacement, and cutover resume paths.
- `metadata.go` and `kube_helpers.go` contain serialization, generated names,
  guarded patches/deletes, waits, and pod-to-workload resolution.
- `dry_run.go` projects the same lifecycle without mutating the cluster.

The intended phase order remains prepare, optional initial sync, quiesce, final
sync, cutover, then workload restore. `AnnState` records the durable boundary
between phases; the migration struct is discovery output, not the source of
truth for resume.

## Safety Invariants

- Unknown migration states and malformed selection filters fail closed.
- Temporary PVCs, final PVCs, sync pods, restore ConfigMaps, and workload
  controllers are identity-checked before existing objects are reused.
- Quiesce records are namespace-scoped and persist workload/pod UIDs so a
  delete/recreate race cannot redirect a retry to a same-name replacement.
- Destructive deletes use UID preconditions. JSON patches test object UID and
  resource version, and claimRef removal also tests the recorded claim identity.
- The destination PV receives the original PVC and quiesce records before both
  PVC objects can disappear.
- Every cutover resume path uses the same final-PVC constructor and honors the
  reclaim-policy restore option.
- Generated sync pod names retain their UID-derived hash even when truncated.

## Deliberate Operational Boundaries

- The RBAC role is necessarily broad because the tool patches and deletes PVCs,
  PVs, pods, and supported controllers.
- A writer created after the final consumer check can still race cutover.
  External reconcilers that can create writers must be paused by the operator.
- The rclone container runs as root to preserve filesystem ownership and mode.
- Dry-run is a local projection; it cannot prove admission, RBAC, scheduling,
  storage binding, or Pod Security behavior.
- Filesystem quiescence is not application-aware consistency. Databases may
  still require application-level flush or shutdown procedures.
- Retained source PVs and StatefulSet restore ConfigMaps remain operator-managed
  recovery artifacts, as documented in the README.

## Verification

The repository is expected to pass:

```sh
make test
GOCACHE=/tmp/scmigrate-go-cache go vet ./...
GOCACHE=/tmp/scmigrate-go-cache go test -race ./...
make build
make e2e
```

The kind-backed suite covers Deployment, StatefulSet, DaemonSet, shared-PVC,
multi-parent, three-replica StatefulSet, and live etcd migration scenarios.
