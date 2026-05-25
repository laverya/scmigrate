package scmigrate

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Options struct {
	Namespace            string
	AllNamespaces        bool
	Selector             string
	AnnotationFilters    []string
	SourceStorageClass   string
	TargetStorageClass   string
	RunnerImage          string
	RsyncArgs            string
	Yes                  bool
	DryRun               bool
	SkipInitialSync      bool
	RestoreReclaimPolicy bool
}

type Migration struct {
	Source      *corev1.PersistentVolumeClaim
	SourcePV    *corev1.PersistentVolume
	Destination *corev1.PersistentVolumeClaim
	DestPV      *corev1.PersistentVolume
	State       string
	Consumers   []PVCConsumer
}

type WorkloadRef struct {
	Kind      string
	Namespace string
	Name      string
}

type PVCConsumer struct {
	Pod      corev1.Pod
	Workload WorkloadRef
}

type StoredPVC struct {
	Name             string                              `json:"name"`
	Namespace        string                              `json:"namespace"`
	Labels           map[string]string                   `json:"labels,omitempty"`
	Annotations      map[string]string                   `json:"annotations,omitempty"`
	AccessModes      []corev1.PersistentVolumeAccessMode `json:"accessModes"`
	VolumeMode       *corev1.PersistentVolumeMode        `json:"volumeMode,omitempty"`
	StorageRequest   string                              `json:"storageRequest"`
	StorageClassName *string                             `json:"storageClassName,omitempty"`
	Selector         *metav1.LabelSelector               `json:"selector,omitempty"`
}

type QuiesceRecord struct {
	Kind              string                         `json:"kind"`
	Name              string                         `json:"name"`
	Namespace         string                         `json:"namespace"`
	OriginalReplicas  int32                          `json:"originalReplicas"`
	PodName           string                         `json:"podName"`
	PodNames          []string                       `json:"podNames,omitempty"`
	NodeName          string                         `json:"nodeName,omitempty"`
	NodeNames         []string                       `json:"nodeNames,omitempty"`
	StatefulSet       *appsv1.StatefulSet            `json:"statefulSet,omitempty"`
	DaemonSetStrategy appsv1.DaemonSetUpdateStrategy `json:"daemonSetStrategy,omitempty"`
	DaemonSetAffinity *corev1.Affinity               `json:"daemonSetAffinity,omitempty"`
}
