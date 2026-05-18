package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	sourceStorageClass = "scmigrate-e2e-source"
	targetStorageClass = "scmigrate-e2e-target"
)

type e2eCase struct {
	name       string
	kind       string
	claimName  string
	podLabel   string
	rolloutRef string
	manifest   func(namespace, runID, image string) string
}

func TestMigrations(t *testing.T) {
	if os.Getenv("SCMIGRATE_E2E") != "1" {
		t.Skip("set SCMIGRATE_E2E=1 to run kind-backed e2e tests")
	}
	requireEnv(t, "KUBECONFIG")
	requireEnv(t, "SCMIGRATE_BIN")
	image := requireEnv(t, "SCMIGRATE_RUNNER_IMAGE")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	applyYAML(t, ctx, storageClassesYAML())

	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	cases := []e2eCase{
		{
			name:       "deployment",
			kind:       "Deployment",
			claimName:  "data",
			podLabel:   "app=scmigrate-e2e-deployment",
			rolloutRef: "deployment/app",
			manifest:   deploymentYAML,
		},
		{
			name:       "statefulset",
			kind:       "StatefulSet",
			claimName:  "data-app-0",
			podLabel:   "app=scmigrate-e2e-statefulset",
			rolloutRef: "statefulset/app",
			manifest:   statefulSetYAML,
		},
		{
			name:       "daemonset",
			kind:       "DaemonSet",
			claimName:  "data",
			podLabel:   "app=scmigrate-e2e-daemonset",
			rolloutRef: "daemonset/app",
			manifest:   daemonSetYAML,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			namespace := "scmigrate-e2e-" + tc.name
			createNamespace(t, ctx, namespace)
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				_ = kubectl(cleanupCtx, "delete", "namespace", namespace, "--ignore-not-found=true")
			})

			applyYAML(t, ctx, tc.manifest(namespace, runID, image))
			kubectlOK(t, ctx, "rollout", "status", "-n", namespace, tc.rolloutRef, "--timeout=180s")

			pod := podName(t, ctx, namespace, tc.podLabel)
			proof := fmt.Sprintf("%s proof %s", tc.kind, runID)
			kubectlOK(t, ctx, "exec", "-n", namespace, pod, "--", "sh", "-c", fmt.Sprintf("printf %%s %q > /data/proof.txt && sync", proof))
			assertPodFile(t, ctx, namespace, pod, proof)

			runScmigrate(t, ctx, namespace, tc.name, runID, image)

			kubectlOK(t, ctx, "rollout", "status", "-n", namespace, tc.rolloutRef, "--timeout=240s")
			assertPVC(t, ctx, namespace, tc.claimName)
			restoredPod := podName(t, ctx, namespace, tc.podLabel)
			assertPodFile(t, ctx, namespace, restoredPod, proof)
		})
	}
}

func TestThreeReplicaStatefulSetEmptyPVCs(t *testing.T) {
	if os.Getenv("SCMIGRATE_E2E") != "1" {
		t.Skip("set SCMIGRATE_E2E=1 to run kind-backed e2e tests")
	}
	requireEnv(t, "KUBECONFIG")
	requireEnv(t, "SCMIGRATE_BIN")
	image := requireEnv(t, "SCMIGRATE_RUNNER_IMAGE")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	applyYAML(t, ctx, storageClassesYAML())

	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	namespace := "scmigrate-e2e-statefulset-3"
	createNamespace(t, ctx, namespace)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = kubectl(cleanupCtx, "delete", "namespace", namespace, "--ignore-not-found=true")
	})

	applyYAML(t, ctx, threeReplicaStatefulSetYAML(namespace, runID, image))
	kubectlOK(t, ctx, "rollout", "status", "-n", namespace, "statefulset/app", "--timeout=240s")

	runScmigrate(t, ctx, namespace, "statefulset-empty", runID, image)

	kubectlOK(t, ctx, "rollout", "status", "-n", namespace, "statefulset/app", "--timeout=240s")
	for i := 0; i < 3; i++ {
		assertPVC(t, ctx, namespace, fmt.Sprintf("data-app-%d", i))
	}
}

func TestEtcdThreeReplicaStatefulSetReadsWritesDuringMigration(t *testing.T) {
	if os.Getenv("SCMIGRATE_E2E") != "1" {
		t.Skip("set SCMIGRATE_E2E=1 to run kind-backed e2e tests")
	}
	requireEnv(t, "KUBECONFIG")
	requireEnv(t, "SCMIGRATE_BIN")
	runnerImage := requireEnv(t, "SCMIGRATE_RUNNER_IMAGE")
	etcdImage := requireEnv(t, "ETCD_IMAGE")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	applyYAML(t, ctx, storageClassesYAML())

	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	namespace := "scmigrate-e2e-etcd"
	createNamespace(t, ctx, namespace)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = kubectl(cleanupCtx, "delete", "namespace", namespace, "--ignore-not-found=true")
	})

	applyYAML(t, ctx, etcdStatefulSetYAML(namespace, runID, etcdImage))
	kubectlOK(t, ctx, "rollout", "status", "-n", namespace, "statefulset/etcd", "--timeout=300s")
	etcdPutGet(t, ctx, namespace, "/scmigrate/seed", "seed-"+runID)

	stopWrites := make(chan struct{})
	writerErr := make(chan error, 1)
	var writeCount atomic.Int64
	go func() {
		writerErr <- runEtcdWriter(ctx, namespace, runID, stopWrites, &writeCount)
	}()

	migrationErr := runScmigrateE(ctx, namespace, "etcd", runID, runnerImage)
	close(stopWrites)
	writerResult := <-writerErr
	if migrationErr != nil {
		t.Fatal(migrationErr)
	}
	if writerResult != nil {
		t.Fatal(writerResult)
	}
	if writeCount.Load() < 3 {
		t.Fatalf("expected at least 3 successful writes during migration, got %d", writeCount.Load())
	}

	kubectlOK(t, ctx, "rollout", "status", "-n", namespace, "statefulset/etcd", "--timeout=300s")
	for i := 0; i < 3; i++ {
		assertPVC(t, ctx, namespace, fmt.Sprintf("data-etcd-%d", i))
	}
	etcdPutGet(t, ctx, namespace, "/scmigrate/after", "after-"+runID)
	got := etcdGet(t, ctx, namespace, "/scmigrate/seed")
	if got != "seed-"+runID {
		t.Fatalf("seed value = %q, want %q", got, "seed-"+runID)
	}
	if _, err := etcdExecE(ctx, namespace, "etcdctl", "--endpoints=http://etcd-client:2379", "endpoint", "health"); err != nil {
		t.Fatal(err)
	}
}

func runEtcdWriter(ctx context.Context, namespace, runID string, stop <-chan struct{}, count *atomic.Int64) error {
	ticker := time.NewTicker(750 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			next := count.Load() + 1
			value := fmt.Sprintf("live-%s-%d", runID, next)
			if err := eventually(ctx, 30*time.Second, func(attempt context.Context) error {
				return etcdPutGetE(attempt, namespace, "/scmigrate/live", value)
			}); err != nil {
				return fmt.Errorf("continuous etcd read/write failed: %w", err)
			}
			count.Add(1)
		}
	}
}

func eventually(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	deadline, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		attempt, attemptCancel := context.WithTimeout(deadline, 10*time.Second)
		lastErr = fn(attempt)
		attemptCancel()
		if lastErr == nil {
			return nil
		}
		select {
		case <-deadline.Done():
			return lastErr
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("%s is required", key)
	}
	return value
}

func createNamespace(t *testing.T, ctx context.Context, namespace string) {
	t.Helper()
	kubectlOK(t, ctx, "delete", "namespace", namespace, "--ignore-not-found=true", "--wait=true", "--timeout=120s")
	kubectlOK(t, ctx, "create", "namespace", namespace)
}

func runScmigrate(t *testing.T, ctx context.Context, namespace, caseName, runID, image string) {
	t.Helper()
	output, err := runScmigrateCommand(ctx, namespace, caseName, runID, image)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, "Migrating "+namespace+"/") {
		t.Fatalf("scmigrate did not migrate any PVCs; output:\n%s", output)
	}
}

func runScmigrateE(ctx context.Context, namespace, caseName, runID, image string) error {
	output, err := runScmigrateCommand(ctx, namespace, caseName, runID, image)
	if err != nil {
		return err
	}
	if !strings.Contains(output, "Migrating "+namespace+"/") {
		return fmt.Errorf("scmigrate did not migrate any PVCs; output:\n%s", output)
	}
	return nil
}

func runScmigrateCommand(ctx context.Context, namespace, caseName, runID, image string) (string, error) {
	selector := fmt.Sprintf("scmigrate-e2e=%s,scmigrate-e2e-run=%s", caseName, runID)
	bin := os.Getenv("SCMIGRATE_BIN")
	if bin == "" {
		return "", fmt.Errorf("SCMIGRATE_BIN is required")
	}
	return runCommandE(ctx, bin, []string{
		"run",
		"--namespace", namespace,
		"--selector", selector,
		"--source-storage-class", sourceStorageClass,
		"--target-storage-class", targetStorageClass,
		"--runner-image", image,
		"--rsync-args", "-a --delete",
		"--yes",
	})
}

func assertPVC(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	storageClass := strings.TrimSpace(kubectlOK(t, ctx, "get", "pvc", "-n", namespace, name, "-o", "jsonpath={.spec.storageClassName}"))
	if storageClass != targetStorageClass {
		t.Fatalf("pvc/%s storageClass = %q, want %q", name, storageClass, targetStorageClass)
	}
	state := strings.TrimSpace(kubectlOK(t, ctx, "get", "pvc", "-n", namespace, name, "-o", "jsonpath={.metadata.annotations.scmigrate\\.laverya\\.github\\.com/state}"))
	if state != "restored" {
		t.Fatalf("pvc/%s migration state = %q, want restored", name, state)
	}
}

func assertPodFile(t *testing.T, ctx context.Context, namespace, pod, want string) {
	t.Helper()
	got := strings.TrimSpace(kubectlOK(t, ctx, "exec", "-n", namespace, pod, "--", "cat", "/data/proof.txt"))
	if got != want {
		t.Fatalf("proof data = %q, want %q", got, want)
	}
}

func etcdPutGet(t *testing.T, ctx context.Context, namespace, key, value string) {
	t.Helper()
	if err := etcdPutGetE(ctx, namespace, key, value); err != nil {
		t.Fatal(err)
	}
}

func etcdPutGetE(ctx context.Context, namespace, key, value string) error {
	if _, err := etcdExecE(ctx, namespace, "etcdctl", "--endpoints=http://etcd-client:2379", "put", key, value); err != nil {
		return err
	}
	out, err := etcdExecE(ctx, namespace, "etcdctl", "--endpoints=http://etcd-client:2379", "get", key, "--print-value-only")
	if err != nil {
		return err
	}
	if got := strings.TrimSpace(out); got != value {
		return fmt.Errorf("etcd value for %s = %q, want %q", key, got, value)
	}
	return nil
}

func etcdGet(t *testing.T, ctx context.Context, namespace, key string) string {
	t.Helper()
	out, err := etcdExecE(ctx, namespace, "etcdctl", "--endpoints=http://etcd-client:2379", "get", key, "--print-value-only")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(out)
}

func etcdExecE(ctx context.Context, namespace string, args ...string) (string, error) {
	var failures []string
	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("etcd-%d", i)
		kubectlArgs := append([]string{"exec", "-n", namespace, pod, "--"}, args...)
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		out, err := runCommandE(attempt, "kubectl", kubectlArgs)
		cancel()
		if err == nil {
			return out, nil
		}
		failures = append(failures, fmt.Sprintf("%s: %v", pod, err))
	}
	return "", fmt.Errorf("all etcd exec attempts failed: %s", strings.Join(failures, "\n"))
}

func podName(t *testing.T, ctx context.Context, namespace, label string) string {
	t.Helper()
	out := strings.TrimSpace(kubectlOK(t, ctx, "get", "pods", "-n", namespace, "-l", label, "-o", "jsonpath={.items[0].metadata.name}"))
	if out == "" {
		t.Fatalf("no pod found for label %q in namespace %q", label, namespace)
	}
	return out
}

func applyYAML(t *testing.T, ctx context.Context, yaml string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Env = commandEnv()
	cmd.Stdin = strings.NewReader(yaml)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl apply failed: %v\n%s", err, out.String())
	}
}

func kubectlOK(t *testing.T, ctx context.Context, args ...string) string {
	t.Helper()
	return runCommand(t, ctx, "kubectl", args)
}

func kubectl(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Env = commandEnv()
	return cmd.Run()
}

func runCommand(t *testing.T, ctx context.Context, name string, args []string) string {
	t.Helper()
	out, err := runCommandE(ctx, name, args)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func runCommandE(ctx context.Context, name string, args []string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = commandEnv()
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%s %s failed: %v\n%s", name, strings.Join(args, " "), err, out.String())
	}
	return out.String(), nil
}

func commandEnv() []string {
	env := os.Environ()
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		env = append(env, "KUBECONFIG="+kubeconfig)
	}
	return env
}

func storageClassesYAML() string {
	return fmt.Sprintf(`
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
provisioner: rancher.io/local-path
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
provisioner: rancher.io/local-path
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
`, sourceStorageClass, targetStorageClass)
}

func deploymentYAML(namespace, runID, image string) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: %[1]s
  labels:
    scmigrate-e2e: deployment
    scmigrate-e2e-run: "%[2]s"
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: %[4]s
  resources:
    requests:
      storage: 64Mi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: %[1]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: scmigrate-e2e-deployment
  template:
    metadata:
      labels:
        app: scmigrate-e2e-deployment
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: app
        image: %[3]s
        imagePullPolicy: IfNotPresent
        command: ["sh", "-c", "trap 'exit 0' TERM INT; sleep 86400 & wait"]
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: data
`, namespace, runID, image, sourceStorageClass)
}

func statefulSetYAML(namespace, runID, image string) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Service
metadata:
  name: app
  namespace: %[1]s
spec:
  clusterIP: None
  selector:
    app: scmigrate-e2e-statefulset
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: app
  namespace: %[1]s
spec:
  serviceName: app
  replicas: 1
  selector:
    matchLabels:
      app: scmigrate-e2e-statefulset
  template:
    metadata:
      labels:
        app: scmigrate-e2e-statefulset
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: app
        image: %[3]s
        imagePullPolicy: IfNotPresent
        command: ["sh", "-c", "trap 'exit 0' TERM INT; sleep 86400 & wait"]
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
      labels:
        scmigrate-e2e: statefulset
        scmigrate-e2e-run: "%[2]s"
    spec:
      accessModes: [ReadWriteOnce]
      storageClassName: %[4]s
      resources:
        requests:
          storage: 64Mi
`, namespace, runID, image, sourceStorageClass)
}

func daemonSetYAML(namespace, runID, image string) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: %[1]s
  labels:
    scmigrate-e2e: daemonset
    scmigrate-e2e-run: "%[2]s"
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: %[4]s
  resources:
    requests:
      storage: 64Mi
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: app
  namespace: %[1]s
spec:
  selector:
    matchLabels:
      app: scmigrate-e2e-daemonset
  template:
    metadata:
      labels:
        app: scmigrate-e2e-daemonset
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: app
        image: %[3]s
        imagePullPolicy: IfNotPresent
        command: ["sh", "-c", "trap 'exit 0' TERM INT; sleep 86400 & wait"]
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: data
`, namespace, runID, image, sourceStorageClass)
}

func threeReplicaStatefulSetYAML(namespace, runID, image string) string {
	return fmt.Sprintf(`
apiVersion: v1
kind: Service
metadata:
  name: app
  namespace: %[1]s
spec:
  clusterIP: None
  selector:
    app: scmigrate-e2e-statefulset-empty
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: app
  namespace: %[1]s
spec:
  serviceName: app
  replicas: 3
  selector:
    matchLabels:
      app: scmigrate-e2e-statefulset-empty
  template:
    metadata:
      labels:
        app: scmigrate-e2e-statefulset-empty
    spec:
      terminationGracePeriodSeconds: 0
      containers:
      - name: app
        image: %[3]s
        imagePullPolicy: IfNotPresent
        command: ["sh", "-c", "trap 'exit 0' TERM INT; sleep 86400 & wait"]
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
      labels:
        scmigrate-e2e: statefulset-empty
        scmigrate-e2e-run: "%[2]s"
    spec:
      accessModes: [ReadWriteOnce]
      storageClassName: %[4]s
      resources:
        requests:
          storage: 64Mi
`, namespace, runID, image, sourceStorageClass)
}

func etcdStatefulSetYAML(namespace, runID, image string) string {
	initialCluster := fmt.Sprintf("etcd-0=http://etcd-0.etcd.%[1]s.svc.cluster.local:2380,etcd-1=http://etcd-1.etcd.%[1]s.svc.cluster.local:2380,etcd-2=http://etcd-2.etcd.%[1]s.svc.cluster.local:2380", namespace)
	return fmt.Sprintf(`
apiVersion: v1
kind: Service
metadata:
  name: etcd
  namespace: %[1]s
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: scmigrate-e2e-etcd
  ports:
  - name: client
    port: 2379
  - name: peer
    port: 2380
---
apiVersion: v1
kind: Service
metadata:
  name: etcd-client
  namespace: %[1]s
spec:
  selector:
    app: scmigrate-e2e-etcd
  ports:
  - name: client
    port: 2379
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: etcd
  namespace: %[1]s
spec:
  serviceName: etcd
  replicas: 3
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      app: scmigrate-e2e-etcd
  template:
    metadata:
      labels:
        app: scmigrate-e2e-etcd
    spec:
      terminationGracePeriodSeconds: 10
      containers:
      - name: etcd
        image: %[3]s
        imagePullPolicy: IfNotPresent
        command:
        - etcd
        args:
        - --name=$(POD_NAME)
        - --data-dir=/var/lib/etcd
        - --listen-client-urls=http://0.0.0.0:2379
        - --advertise-client-urls=http://$(POD_NAME).etcd.$(POD_NAMESPACE).svc.cluster.local:2379
        - --listen-peer-urls=http://0.0.0.0:2380
        - --initial-advertise-peer-urls=http://$(POD_NAME).etcd.$(POD_NAMESPACE).svc.cluster.local:2380
        - --initial-cluster=%[5]s
        - --initial-cluster-state=new
        - --initial-cluster-token=scmigrate-e2e-%[2]s
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        ports:
        - name: client
          containerPort: 2379
        - name: peer
          containerPort: 2380
        readinessProbe:
          exec:
            command:
            - etcdctl
            - --endpoints=http://127.0.0.1:2379
            - endpoint
            - health
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 12
        volumeMounts:
        - name: data
          mountPath: /var/lib/etcd
  volumeClaimTemplates:
  - metadata:
      name: data
      labels:
        scmigrate-e2e: etcd
        scmigrate-e2e-run: "%[2]s"
    spec:
      accessModes: [ReadWriteOnce]
      storageClassName: %[4]s
      resources:
        requests:
          storage: 128Mi
`, namespace, runID, image, sourceStorageClass, initialCluster)
}
