package scmigrate

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (r *Runner) prepare(ctx context.Context, source *corev1.PersistentVolumeClaim) error {
	if err := validateSourcePVC(source); err != nil {
		return err
	}
	tempName := tempPVCName(source)
	snapshot, err := pvcSnapshot(source)
	if err != nil {
		return err
	}
	nextState := source.Annotations[AnnState]
	before, err := stateBefore(nextState, StatePrepared)
	if err != nil {
		return err
	}
	if before {
		nextState = StatePrepared
	}
	if err := r.patchPVCAnnotations(ctx, source, map[string]string{
		AnnState:              nextState,
		AnnDestinationPVC:     tempName,
		AnnSourcePV:           source.Spec.VolumeName,
		AnnTargetStorageClass: r.opts.TargetStorageClass,
		AnnOriginalPVC:        snapshot,
	}); err != nil {
		return err
	}

	if _, err := r.destinationPVC(ctx, source); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	request := source.Spec.Resources.Requests[corev1.ResourceStorage]
	dest := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tempName,
			Namespace: source.Namespace,
			Labels: map[string]string{
				LabelManagedBy: ManagedByValue,
				LabelRole:      "destination",
				LabelSourceUID: string(source.UID),
			},
			Annotations: map[string]string{
				AnnState:              StatePrepared,
				AnnSourcePVC:          source.Name,
				AnnSourceNamespace:    source.Namespace,
				AnnSourceUID:          string(source.UID),
				AnnSourcePV:           source.Spec.VolumeName,
				AnnTargetStorageClass: r.opts.TargetStorageClass,
				AnnOriginalPVC:        snapshot,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      source.Spec.AccessModes,
			VolumeMode:       source.Spec.VolumeMode,
			StorageClassName: &r.opts.TargetStorageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: request},
			},
		},
	}
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: create destination pvc/%s in %s\n", dest.Name, dest.Namespace)
		return nil
	}
	created, err := r.client.CoreV1().PersistentVolumeClaims(source.Namespace).Create(ctx, dest, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	if err == nil {
		fmt.Fprintf(r.out, "created destination pvc/%s\n", created.Name)
	}
	return nil
}

func (r *Runner) runSync(ctx context.Context, source *corev1.PersistentVolumeClaim, phase string) error {
	if err := validateRunnerImage(r.opts.RunnerImage); err != nil {
		return err
	}
	dest, err := r.destinationPVC(ctx, source)
	if err != nil {
		return err
	}
	podName := syncPodName(source, phase)
	if r.opts.DryRun {
		fmt.Fprintf(r.out, "dry-run: run %s sync pod/%s from pvc/%s to pvc/%s\n", phase, podName, source.Name, dest.Name)
		return nil
	}
	if old, err := r.client.CoreV1().Pods(source.Namespace).Get(ctx, podName, metav1.GetOptions{}); err == nil {
		if old.Status.Phase == corev1.PodSucceeded {
			fmt.Fprintf(r.out, "%s sync already completed by pod/%s\n", phase, podName)
			return r.cleanupSyncPod(ctx, source.Namespace, podName)
		}
		if old.Status.Phase == corev1.PodFailed {
			_ = r.deletePod(ctx, old)
			if err := r.waitPodGone(ctx, source.Namespace, podName); err != nil {
				return err
			}
		} else {
			if err := r.waitPodSucceeded(ctx, source.Namespace, podName); err != nil {
				return err
			}
			return r.cleanupSyncPod(ctx, source.Namespace, podName)
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	command, args, err := rcloneCommand(r.opts.RcloneArgs)
	if err != nil {
		return err
	}
	nodeName := ""
	if phase == SyncPhaseInitial {
		// For WaitForFirstConsumer or topology-constrained storage, the initial
		// sync pod should bind near the sole active writer when there is one.
		nodeName, err = r.syncNodeName(ctx, source)
		if err != nil {
			return err
		}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: source.Namespace,
			Labels: map[string]string{
				LabelManagedBy: ManagedByValue,
				LabelRole:      "sync",
				LabelSourceUID: string(source.UID),
			},
			Annotations: map[string]string{
				AnnSourcePVC:          source.Name,
				AnnDestinationPVC:     dest.Name,
				AnnTargetStorageClass: r.opts.TargetStorageClass,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            "rclone",
				Image:           r.opts.RunnerImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         command,
				Args:            args,
				SecurityContext: &corev1.SecurityContext{
					RunAsUser:  ptrInt64(0),
					RunAsGroup: ptrInt64(0),
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "source", MountPath: "/source", ReadOnly: true},
					{Name: "destination", MountPath: "/destination"},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "source", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: source.Name, ReadOnly: true}}},
				{Name: "destination", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: dest.Name}}},
			},
		},
	}
	if nodeName != "" {
		pod.Spec.Affinity = affinityForNode(nodeName)
	}
	if _, err := r.client.CoreV1().Pods(source.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return err
	}
	fmt.Fprintf(r.out, "started %s sync pod/%s\n", phase, podName)
	if err := r.waitPodSucceeded(ctx, source.Namespace, podName); err != nil {
		return err
	}
	return r.cleanupSyncPod(ctx, source.Namespace, podName)
}

func (r *Runner) cleanupSyncPod(ctx context.Context, namespace, name string) error {
	if err := r.deletePodByName(ctx, namespace, name); err != nil {
		return err
	}
	return r.waitPodGone(ctx, namespace, name)
}

func (r *Runner) syncNodeName(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, error) {
	consumers, err := r.consumingPods(ctx, pvc)
	if err != nil {
		return "", err
	}
	if len(consumers) == 0 {
		return "", nil
	}
	nodeName := consumers[0].Spec.NodeName
	if nodeName == "" {
		return "", nil
	}
	for _, consumer := range consumers[1:] {
		if consumer.Spec.NodeName != nodeName {
			return "", nil
		}
	}
	return nodeName, nil
}

func affinityForNode(nodeName string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchFields: []corev1.NodeSelectorRequirement{{
						Key:      "metadata.name",
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{nodeName},
					}},
				}},
			},
		},
	}
}

func rcloneCommand(rawArgs string) ([]string, []string, error) {
	args := append([]string{"sync"}, strings.Fields(rawArgs)...)
	args = append(args, "/source", "/destination")
	return []string{"/kubectl-scmigrate", "rclone"}, args, nil
}

func validateRunnerImage(image string) error {
	image = strings.TrimSpace(image)
	if image == "" {
		return fmt.Errorf("--runner-image is required for run; use a non-latest tag or digest")
	}
	hasDigest := hasDigestReference(image)
	if strings.Contains(image, "@") && !hasDigest {
		return fmt.Errorf("--runner-image digest references must use image@algorithm:value")
	}
	if hasDigest {
		return nil
	}
	tag := imageTag(image)
	if tag == "" {
		return fmt.Errorf("--runner-image must include a non-latest tag or digest")
	}
	if tag == "latest" {
		return fmt.Errorf("--runner-image must not use the latest tag; use a version tag or digest")
	}
	return nil
}

func imageTag(image string) string {
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon <= lastSlash {
		return ""
	}
	return image[lastColon+1:]
}

func hasDigestReference(image string) bool {
	name, digest, found := strings.Cut(image, "@")
	if !found || name == "" || digest == "" || strings.Contains(digest, "@") {
		return false
	}
	algorithm, encoded, found := strings.Cut(digest, ":")
	return found && algorithm != "" && encoded != ""
}
