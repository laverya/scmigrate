# scmigrate Review Issues

This repository is new code, so the notes below focus on what is likely to
break in real Kubernetes usage, what is not obvious from the current behavior,
and what needs stronger documentation.

## Likely to Break

### P0: Cutover has an unrecoverable interruption window

Status: Resolved.

`pkg/scmigrate/runner.go` deletes the source PVC and the temporary
destination PVC before creating the final PVC:

- `cutover` deletes the source PVC.
- `cutover` deletes the temporary destination PVC.
- Only after both are gone does it clear the destination PV `claimRef` and
  create the final PVC.

If the process dies, the client loses connectivity, RBAC blocks final creation,
or the final PVC is rejected in this window, resume can only look for either the
temporary destination PVC or an existing final PVC. The object that held the
original PVC snapshot may already be gone, so the migration can get stuck with
no durable in-cluster reconstruction path.

Suggested direction: keep a durable cutover record somewhere that survives temp
PVC deletion, or create a replacement final PVC using a sequence that does not
destroy the last copy of the stored spec before the final object exists.

Resolution: the destination PV is annotated with the original PVC snapshot and
quiesce record before either PVC is deleted, and resume can recreate the final
PVC from that PV cutover record if both PVC objects are gone.

### P1: Multi-replica StatefulSet migration likely fails after the first ordinal

Status: Resolved.

The code sorts PVCs by descending ordinal and requires each single-pod
StatefulSet PVC to belong to the current highest ordinal. After migrating
`data-app-2`, the code restores the StatefulSet to its original replica count,
so pod `app-2` comes back. The next PVC, `data-app-1`, is no longer the highest
ordinal and migration errors.

This means the intended one-pod-at-a-time StatefulSet flow probably only works
for the first PVC in a multi-replica StatefulSet.

Suggested direction: keep the StatefulSet scaled/orphaned state coordinated
across all selected PVCs for that StatefulSet, or adjust the ordinal logic so
already-migrated higher ordinals do not block lower ordinals.

Resolution: the single-pod StatefulSet path no longer requires the consumer to
be the highest ordinal, and the restore object is persisted outside the PVC
annotation so each ordinal can be migrated independently.

### P1: The shipped RBAC cannot run the StatefulSet path

Status: Resolved.

`deploy/rbac.yaml` grants `get`, `list`, and `patch` on StatefulSets, but the
implementation deletes and recreates StatefulSets during the orphan-and-restore
path. An in-cluster runner using the provided RBAC will fail on StatefulSet
migrations.

Suggested direction: add the required `delete` and `create` verbs for
StatefulSets, or change the implementation to avoid those verbs.

Resolution: RBAC now grants the StatefulSet create/delete verbs needed by the
orphan-and-restore path, and includes ConfigMap access for StatefulSet restore
records.

### P1: `--dry-run` is not a usable fresh-run preview

Status: Resolved.

On a fresh PVC, `prepare` prints the destination PVC creation instead of
creating it. The migration then reloads unchanged cluster state and the sync
phase tries to look up the missing destination PVC. The dry run stops early
instead of showing the full mutation sequence.

Suggested direction: make dry-run operate on an in-memory projected state, or
make `plan` the explicit full preview and keep `--dry-run` documented as a
single-step mutation guard.

Resolution: dry-run migration now projects the migration phases without
requiring the destination PVC to exist in the cluster.

### P1: The Docker build stage is pinned below the module Go version

Status: Resolved.

`go.mod` requires Go 1.26, while the deleted plugin image `Dockerfile` used
`golang:1.22`. Container builds would fail or rely on toolchain auto-download
behavior rather than the declared build image.

Suggested direction: update the Docker build image to a Go version compatible
with `go.mod`, or lower `go.mod` if the code and dependencies support it.

Resolution: the plugin image `Dockerfile` was removed; releases now publish the
rsync runner image from `Dockerfile.rsync`.

### P2: Some PVC types are copied but cannot actually be synced

Status: Resolved.

The migration preserves `VolumeMode`, but sync pods always use filesystem
`VolumeMounts`. Block-mode PVCs should be rejected before migration because
they need `VolumeDevices` and a different copy strategy. Similarly,
`ReadWriteOncePod` access mode can block the live initial sync because the
source PVC is already mounted by the workload pod.

Suggested direction: add preflight validation for unsupported volume modes and
access modes, with clear errors.

Resolution: block-mode PVCs and `ReadWriteOncePod` PVCs are rejected during
discovery and preparation with explicit errors.

### P2: Resume can drift if rerun with a different target storage class

Status: Resolved.

Resumable destination discovery checks selector, source storage class, and
annotation filters, but it does not check the stored target storage class. A
rerun with a different `--target-storage-class` can resume an old migration and
create the final PVC using the new flag value, even though the destination PV
was provisioned for the old target class.

Suggested direction: compare `AnnTargetStorageClass` on resumable objects with
the current option and fail loudly on mismatch.

Resolution: resumable PVC and PV state is validated against the requested target
storage class and fails fast on mismatches.

### P2: Full StatefulSet specs stored in annotations can exceed Kubernetes limits

Status: Resolved.

The quiesce record can embed an entire StatefulSet object and then store that
JSON in PVC annotations. Real StatefulSets with large environment variables,
labels, annotations, or projected config can exceed Kubernetes annotation size
limits.

Suggested direction: store only the fields required to restore the StatefulSet,
or persist the restore record in a dedicated object with better size
characteristics.

Resolution: StatefulSet restore specs are stored in migration-managed
ConfigMaps, and the quiesce annotation stores only the ConfigMap reference.

## Not Obvious

Status: Resolved in README documentation.

- Supported pod owners are limited to standalone Pods, Deployments,
  ReplicaSets, StatefulSets, and DaemonSets. Jobs, CronJobs, Argo Rollouts,
  operator-owned pods, and custom controllers abort migration.
- Operators, HPAs, and other reconcilers can fight scale-to-zero operations or
  recreate writers during final sync.
- Pod Security Admission may reject the root rsync pod, especially in
  restricted namespaces.
- PVC selectors are restored onto the final PVC. If the original selector was
  intended for a specific static PV, it may prevent binding to the new
  destination PV.
- Default rsync args include ACL and xattr preservation. Filesystems or storage
  backends without ACL/xattr support may fail unless users override
  `--rsync-args`.
- `--skip-initial-sync` implies the final outage includes the entire data copy,
  not just the delta sync.

## Documentation Gaps

Status: Resolved in README documentation.

- Document the exact controller support matrix and what happens for unsupported
  owners.
- Document required RBAC by workload type, especially StatefulSet delete/create
  requirements.
- Document preflight expectations: bound source PVC, filesystem volume mode,
  supported access modes, target storage class capabilities, Pod Security
  requirements, and reclaim policy behavior.
- Document resume safety boundaries and the exact flags that must remain stable
  across reruns.
- Document operational runbooks for failures at each phase, especially after
  quiesce and during cutover.
- Document cleanup responsibilities for retained old PVs and temporary
  migration artifacts.

## Verification Performed After Fixes

The local checks passed after the fixes:

```sh
GOCACHE=/tmp/scmigrate-go-cache go test ./...
GOCACHE=/tmp/scmigrate-go-cache go vet ./...
```

The kind-backed e2e tests are still gated by `SCMIGRATE_E2E=1`; the package was
included in `go test ./...`, but those tests remain skipped unless the
environment opts in with Docker, kind, and kubectl.
