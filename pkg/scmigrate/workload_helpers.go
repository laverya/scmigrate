package scmigrate

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func validateQuiesceRecords(records []QuiesceRecord, namespace string) error {
	if len(records) == 0 {
		return fmt.Errorf("quiesce record is empty")
	}
	for i, record := range records {
		if record.Namespace != namespace {
			return fmt.Errorf("quiesce record %d targets namespace %q, expected %q", i, record.Namespace, namespace)
		}
		switch record.Kind {
		case WorkloadKindNone:
		case WorkloadKindPod:
			if len(recordPodNames(record)) == 0 {
				return fmt.Errorf("quiesce record %d for standalone pods has no pod names", i)
			}
		case WorkloadKindDeployment, WorkloadKindReplicaSet, WorkloadKindStatefulSet:
			if record.Name == "" || record.OriginalReplicas < 1 {
				return fmt.Errorf("quiesce record %d for %s is missing a name or replica count", i, record.Kind)
			}
		case WorkloadKindDaemonSet:
			if record.Name == "" || len(recordNodeNames(record)) == 0 {
				return fmt.Errorf("quiesce record %d for DaemonSet is missing a name or node names", i)
			}
		default:
			return fmt.Errorf("quiesce record %d has unsupported kind %q", i, record.Kind)
		}
	}
	return nil
}

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

func podUIDs(pods []corev1.Pod) map[string]types.UID {
	uids := make(map[string]types.UID, len(pods))
	for _, pod := range pods {
		if pod.UID != "" {
			uids[pod.Name] = pod.UID
		}
	}
	return uids
}

func ensureUID(kind, name string, expected, actual types.UID) error {
	if expected != "" && actual != expected {
		return fmt.Errorf("%s/%s was replaced: expected UID %q, found %q", kind, name, expected, actual)
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
