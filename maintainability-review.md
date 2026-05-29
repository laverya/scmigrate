# scmigrate Maintainability Review

This note captures the readability and reviewability issues found in the main
migration code, plus the order to address them.

## Readability Findings

The main migration story is understandable from the README, but harder to audit
in code than it needs to be. `internal/scmigrate/runner.go` contains public
entry points, migration orchestration, discovery, dry-run projection, sync pod
construction, cutover, resume logic, Kubernetes patch helpers, wait helpers, and
pod-to-workload lookup. `internal/scmigrate/workload.go` contains workload
quiesce planning, mutation, restore, and assorted helpers.

The top-level order in `runner.go` starts well: `NewRunner`, `Plan`, `Run`, and
`migrate`. After that, the human story becomes harder to follow. The migration
phase order is:

1. Prepare destination PVC and durable annotations.
2. Run the live initial sync.
3. Quiesce workload writers.
4. Run the final sync.
5. Cut over PV/PVC bindings.
6. Restore workloads.

The implementations of those phases are split across `runner.go` and
`workload.go`, so a reviewer has to jump between files to audit the riskiest
flow.

## Comments

The core implementation has almost no local comments. The README explains the
system, but code that changes reclaim policies, deletes PVCs, clears PV
`claimRef`s, or temporarily mutates controllers should carry short invariant
comments where the risk occurs.

Recommended comment targets:

- `migrate`: phase order and the resume record used by each phase.
- `cutover`: why the destination PV is annotated before deleting PVC objects.
- `runSync`: why the initial sync pod may be pinned to a workload node.
- StatefulSet quiesce: why orphan/delete/recreate is used for one-pod-at-a-time
  migration.
- DaemonSet quiesce: why `OnDelete` plus node affinity exclusions are used.
- `stateBefore`: valid state ordering and invalid-state behavior.

## Ordered Work Items

1. Split `runner.go` by responsibility: migration orchestration, discovery,
   sync pods, cutover/resume, Kubernetes helpers.
2. Reorder `workload.go` around the lifecycle: quiesce planning, quiesce
   application, restore, then helpers.
3. Extract duplicated final-PVC recreation from the normal and resumable
   cutover paths.
4. Replace stringly states and workload/quiesce kinds with named constants,
   including the implicit `"new"` state.
5. Remove or justify unused/test-only helpers and fields, including
   `Runner.namespace`, `defaultKubeconfigPath`, `mergeAnnotations`, and
   convenience quiesce wrappers.
6. Validate unknown migration states instead of treating missing map keys as
   phase zero.
7. Add short invariant comments at risky transitions.
8. Run formatting and the unit test suite.

## Hostile Reviewer Ammunition

A skeptical reviewer who did not want to allow this project would likely focus
on operational risk:

- The shipped RBAC grants broad and destructive access: create, patch, and delete
  permissions on PVCs, PVs, pods, and StatefulSets.
- Cutover intentionally deletes the source PVC and temporary destination PVC,
  clears the destination PV `claimRef`, then recreates the final PVC. The resume
  model reduces the risk, but the operation is still high-impact.
- Consumer discovery now considers all non-terminal pods and rechecks for
  active consumers immediately before cutover. A hostile reviewer can still
  point at the remaining race window between that last recheck and the PVC/PV
  mutations if an external actor creates new writers at exactly the wrong time.
- The highest-risk deletes now use UID preconditions, and controller/PV
  claimRef patches use JSON Patch tests where the current UID is known. A
  reviewer can still ask for wider UID guards on non-destructive metadata and
  reclaim-policy patches.
- The rsync worker still runs as root so it can preserve ownership and
  filesystem metadata, but rsync is now invoked directly with explicit command
  and argument vectors instead of through `sh -c`.
- `run` no longer defaults to `latest`; release-versioned binaries default to
  the matching runner image tag, and dev builds require an explicit non-`latest`
  tag or digest.
- Dry-run is a local textual projection. It does not prove RBAC, admission,
  binding, scheduling, or Pod Security success.
- Application consistency is outside the tool's proof. The tool stops writers
  and copies files, but it does not provide database-aware hooks, fsfreeze, or
  application-level flush verification.
