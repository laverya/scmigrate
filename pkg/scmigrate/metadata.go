package scmigrate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func cloneMap(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func pvcSnapshot(pvc *corev1.PersistentVolumeClaim) (string, error) {
	request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	stored := StoredPVC{
		Name:             pvc.Name,
		Namespace:        pvc.Namespace,
		Labels:           cloneMap(pvc.Labels),
		Annotations:      cloneMap(pvc.Annotations),
		AccessModes:      append([]corev1.PersistentVolumeAccessMode{}, pvc.Spec.AccessModes...),
		VolumeMode:       pvc.Spec.VolumeMode,
		StorageRequest:   request.String(),
		StorageClassName: pvc.Spec.StorageClassName,
		Selector:         pvc.Spec.Selector,
	}
	for k := range stored.Annotations {
		if strings.HasPrefix(k, AnnotationPrefix) || strings.HasPrefix(k, "pv.kubernetes.io/") || strings.HasPrefix(k, "volume.kubernetes.io/") {
			delete(stored.Annotations, k)
		}
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func storedPVC(encoded string) (*StoredPVC, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	var stored StoredPVC
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, err
	}
	return &stored, nil
}

func finalPVCFromStored(stored *StoredPVC, targetSC, volumeName string) (*corev1.PersistentVolumeClaim, error) {
	qty, err := resource.ParseQuantity(stored.StorageRequest)
	if err != nil {
		return nil, err
	}
	labels := cloneMap(stored.Labels)
	annotations := cloneMap(stored.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[AnnState] = StateCutover
	annotations[AnnDestinationPV] = volumeName
	annotations[AnnTargetStorageClass] = targetSC
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1ObjectMeta(stored.Name, stored.Namespace, labels, annotations),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      stored.AccessModes,
			VolumeMode:       stored.VolumeMode,
			StorageClassName: &targetSC,
			Selector:         stored.Selector,
			VolumeName:       volumeName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty},
			},
		},
	}, nil
}

func tempPVCName(pvc *corev1.PersistentVolumeClaim) string {
	base := "scmigrate-" + pvc.Name
	hash := shortHash(string(pvc.UID))
	maxBase := 63 - len(hash) - 1
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	return strings.TrimRight(base, "-") + "-" + hash
}

func syncPodName(pvc *corev1.PersistentVolumeClaim, phase string) string {
	hash := shortHash(string(pvc.UID))
	base := fmt.Sprintf("scmigrate-%s-%s", phase, pvc.Name)
	maxBase := 63 - len(hash) - 1
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	return strings.TrimRight(base, "-") + "-" + hash
}

func shortHash(value string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return fmt.Sprintf("%08x", h.Sum32())
}

func sortedKeys(in map[string]string) []string {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func metav1ObjectMeta(name, namespace string, labels, annotations map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        name,
		Namespace:   namespace,
		Labels:      labels,
		Annotations: annotations,
	}
}
