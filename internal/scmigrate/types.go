package scmigrate

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Options struct {
	Namespace              string
	AllNamespaces          bool
	Selector               string
	AnnotationFilters      []string
	SourceStorageClass     string
	TargetStorageClass     string
	RunnerImage            string
	RsyncArgs              string
	Yes                    bool
	DryRun                 bool
	AllowMultipleConsumers bool
	SkipInitialSync        bool
	RestoreReclaimPolicy   bool
}

type Migration struct {
	Source      *corev1.PersistentVolumeClaim
	SourcePV    *corev1.PersistentVolume
	Destination *corev1.PersistentVolumeClaim
	DestPV      *corev1.PersistentVolume
	State       string
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
	Kind             string `json:"kind"`
	Name             string `json:"name"`
	Namespace        string `json:"namespace"`
	OriginalReplicas int32  `json:"originalReplicas"`
	PodName          string `json:"podName"`
}
