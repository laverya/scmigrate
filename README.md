# scmigrate

`scmigrate` is a `kubectl` plugin and rsync helper image for moving selected
Kubernetes PVCs from one `StorageClass` to another without ever intentionally
leaving the cluster with only an in-flight copy of the data.

The plugin is built around resumable phases. It writes migration state to PVC,
PV, and sync-pod annotations before each cluster mutation that matters. If the
process is interrupted, a later run uses those annotations to continue from the
last durable point.

## Install

Build the kubectl plugin:

```sh
go build -o kubectl-scmigrate ./cmd/kubectl-scmigrate
install -m 0755 kubectl-scmigrate ~/.local/bin/kubectl-scmigrate
```

Build and push the rsync worker image:

```sh
docker build -f Dockerfile.rsync -t ghcr.io/laverya/scmigrate-rsync:latest .
docker push ghcr.io/laverya/scmigrate-rsync:latest
```

Apply RBAC for an in-cluster runner, or grant equivalent rights to the user that
runs the plugin. StatefulSet migrations require permission to delete and
recreate StatefulSets; `scmigrate` also creates a migration-managed ConfigMap
restore record when it uses the StatefulSet orphaning path.

```sh
kubectl create namespace scmigrate-system
kubectl apply -f deploy/rbac.yaml
```

## Usage

Plan a migration:

```sh
kubectl scmigrate plan \
  --namespace default \
  --selector app=postgres \
  --source-storage-class old-sc \
  --target-storage-class new-sc
```

Run it:

```sh
kubectl scmigrate run \
  --namespace default \
  --selector app=postgres \
  --target-storage-class new-sc \
  --runner-image ghcr.io/laverya/scmigrate-rsync:latest \
  --yes
```

PVCs can be selected by namespace, label selector, source storage class, and
repeatable annotation filters:

```sh
kubectl scmigrate plan \
  --all-namespaces \
  --selector 'migration=scmigrate' \
  --annotation scmigrate/approved=true \
  --target-storage-class premium-rwo
```

## Migration Flow

For each matching bound PVC, the plugin:

1. Records the original PVC metadata/spec in annotations.
2. Creates a temporary destination PVC using the target storage class.
3. Starts an rsync pod mounting the source read-only and the destination
   read-write while the workload remains online.
4. Quiesces every running pod that is using that PVC by grouping consumers by
   their owning workload and scaling/deleting each affected parent once.
5. Runs a final rsync pass.
6. Sets both the source PV and destination PV reclaim policies to `Retain`.
7. Deletes the original and temporary PVCs.
8. Clears the destination PV `claimRef`.
9. Recreates the PVC with the original name, labels, and annotations, explicitly
   bound to the destination PV.
10. Restores the workload replica count.

The old PV is deliberately left retained. Delete it manually only after the new
PVC has been verified.

The destination PVC does not have to bind before the first sync pod is created.
That keeps storage classes with `WaitForFirstConsumer` binding from deadlocking.
When exactly one workload pod is using the source PVC, the initial sync pod is
pinned to that same node so local or topology-constrained volumes can bind in
the same place as the source consumer.

## Avoiding Full Service Outages

Final sync requires the PVC to have no active writers. `scmigrate` determines
all running pods that mount each selected PVC, resolves their owning workload,
and quiesces every affected workload before the final sync and cutover.

For `Deployment` and standalone `ReplicaSet` workloads, the plugin scales each
affected parent to zero once per PVC and restores the original replica count
after cutover. This covers PVCs shared by multiple replicas and PVCs mounted by
multiple parent resources.

For `StatefulSet` workloads using ordinary `volumeClaimTemplates`, each PVC is
usually consumed by one pod. The plugin sorts those PVCs by trailing ordinal in
descending order, stores the StatefulSet restore spec in a migration-managed
ConfigMap, temporarily orphans the StatefulSet, deletes the consumer pod, and
recreates the StatefulSet after cutover. This keeps one StatefulSet pod down at
a time. If a StatefulSet shares one PVC across multiple pods, the plugin scales
the StatefulSet to zero and restores the original replica count after cutover.

For `DaemonSet` workloads, the plugin temporarily switches the DaemonSet to
`OnDelete`, adds required node-affinity exclusions for every node with a pod
that is consuming the PVC, and deletes those pods. Other DaemonSet pods keep
running because the strategy change prevents a rolling update. After cutover,
the original affinity and update strategy are restored and the plugin waits for
ready DaemonSet pods on the original nodes.

## Resume Model

All important objects are marked with annotations under:

```text
scmigrate.laverya.github.com/*
```

The temporary destination PVC stores the original PVC metadata and workload
quiesce record. That means a run can resume even after the original PVC has been
deleted but before the final PVC has been recreated.

During cutover, the destination PV is also annotated with the original PVC
snapshot before either PVC is deleted. That lets a later run continue even if
the process stops after both PVC objects are gone but before the final PVC is
created.

Use `kubectl scmigrate run ... --yes` again with the same selection flags to
continue an interrupted migration.

Keep these flags stable when resuming: `--namespace` or `--all-namespaces`,
`--selector`, `--annotation`, `--source-storage-class`, and
`--target-storage-class`. If a resumable object was prepared for a different
target storage class, the plugin stops instead of continuing with mixed target
state.

## Operational Notes

- The source PVC must be bound.
- Only filesystem PVCs are supported. Block-mode PVCs are rejected because the
  rsync worker mounts PVCs as filesystems.
- `ReadWriteOncePod` PVCs are rejected because the live initial sync requires a
  second pod to mount the source PVC.
- The destination storage class must support the requested access modes,
  filesystem volume mode, capacity, and any topology required by the source
  workload.
- PVC selectors are restored on the final PVC. Static PV selectors that only
  matched the old PV can prevent the final PVC from binding to the new PV.
- The rsync image runs as root so it can preserve ownership and mode bits.
  Namespaces with restricted Pod Security Admission must allow the migration
  worker pod or run it under a policy that permits the required filesystem
  operations.
- The default rsync arguments are:

```text
-aHAX --numeric-ids --delete --info=progress2
```

- If the source or target filesystem does not support ACLs or extended
  attributes, override `--rsync-args` to remove `-A` and/or `-X`.
- The plugin leaves PV reclaim policies as `Retain` by default. Pass
  `--restore-reclaim-policy` to restore the destination PV's original policy
  after cutover.
- The old PV is intentionally retained after a successful migration. Remove it
  only after validating the new PVC and application data.
- `--skip-initial-sync` means the whole data copy happens during the final
  outage window.

## Supported Workloads

`scmigrate` can quiesce pods owned by:

- standalone Pods
- Deployments
- standalone ReplicaSets
- StatefulSets
- DaemonSets

Other owners, including Jobs, CronJobs, Argo Rollouts, operator-specific
controllers, and custom controllers, are rejected rather than guessed. If an
operator, HPA, or other reconciler can recreate writers while a workload is
quiesced, pause that reconciler before running the migration.

## Failure Recovery

The migration state is durable across reruns. Use the same command and selection
flags to resume.

- Before quiesce: rerun the migration; it reuses the prepared destination PVC
  and sync pod state.
- After quiesce: rerun the migration promptly. Workloads may intentionally
  remain scaled down or orphaned until restore completes.
- During cutover: rerun the migration. The temporary destination PVC or the
  destination PV cutover record contains the original PVC snapshot needed to
  recreate the final PVC.
- After restore: verify the workload, then clean up retained old PVs and any
  migration-managed ConfigMaps after you no longer need rollback context.

## Development

Run unit tests:

```sh
make test
```

Run the kind-backed end-to-end tests:

```sh
make e2e
```

The e2e target creates a temporary kind cluster, builds and loads the rsync
helper image, then verifies real PVC migrations for a `Deployment`,
`StatefulSet`, and `DaemonSet`. It requires `docker`, `kind`, and `kubectl`.
Set `KEEP_E2E_CLUSTER=1` to leave the cluster running after the test.
