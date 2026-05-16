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
runs the plugin:

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
4. Quiesces only the single pod that is using that PVC.
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

Final sync requires the PVC to have no active writers. By default, `scmigrate`
refuses to finalize a PVC mounted by more than one running pod.

For `Deployment` workloads, the plugin annotates pods with
`controller.kubernetes.io/pod-deletion-cost` and scales the deployment down by
one replica so Kubernetes removes the pod that owns the PVC being migrated. It
scales the deployment back after the cutover.

For `StatefulSet` workloads, Kubernetes can safely remove only the highest
ordinal by scaling down one replica. The plugin sorts migrations by trailing
ordinal in descending order and refuses a lower ordinal until it is the highest
active ordinal. This keeps one StatefulSet pod down at a time.

For `DaemonSet` workloads, the plugin temporarily switches the DaemonSet to
`OnDelete`, adds a required node-affinity exclusion for the node running the pod
that owns the PVC, and deletes only that pod. Other DaemonSet pods keep running
because the strategy change prevents a rolling update. After cutover, the
original affinity and update strategy are restored and the plugin waits for a
ready DaemonSet pod on the original node.

For PVCs mounted by multiple running pods, use external application quiescing or
maintenance-mode logic first. `--allow-multiple-consumers` only disables the
guard; it does not make concurrent writers safe.

## Resume Model

All important objects are marked with annotations under:

```text
scmigrate.laverya.github.com/*
```

The temporary destination PVC stores the original PVC metadata and workload
quiesce record. That means a run can resume even after the original PVC has been
deleted but before the final PVC has been recreated.

Use `kubectl scmigrate run ... --yes` again with the same selection flags to
continue an interrupted migration.

## Operational Notes

- The source PVC must be bound.
- The destination storage class must support the requested access modes and
  volume mode.
- The rsync image runs as root so it can preserve ownership and mode bits.
- The default rsync arguments are:

```text
-aHAX --numeric-ids --delete --info=progress2
```

- The plugin leaves PV reclaim policies as `Retain` by default. Pass
  `--restore-reclaim-policy` to restore the destination PV's original policy
  after cutover.
