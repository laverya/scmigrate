package scmigrate

import (
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func recordPodNames(record QuiesceRecord) []string {
	if len(record.PodNames) > 0 {
		return record.PodNames
	}
	if record.PodName != "" {
		return []string{record.PodName}
	}
	return nil
}

func recordNodeNames(record QuiesceRecord) []string {
	if len(record.NodeNames) > 0 {
		return record.NodeNames
	}
	if record.NodeName != "" {
		return []string{record.NodeName}
	}
	return nil
}

func firstPodName(record QuiesceRecord) string {
	names := recordPodNames(record)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func podReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func controllerOwner(refs []metav1.OwnerReference) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}

func podOrdinal(name string) (int32, bool) {
	idx := strings.LastIndex(name, "-")
	if idx < 0 || idx == len(name)-1 {
		return 0, false
	}
	ordinal, err := strconv.Atoi(name[idx+1:])
	if err != nil {
		return 0, false
	}
	return int32(ordinal), true
}
