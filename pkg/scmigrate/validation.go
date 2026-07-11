package scmigrate

import (
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

func validateOptions(opts Options) error {
	if strings.TrimSpace(opts.TargetStorageClass) == "" {
		return errors.New("--target-storage-class is required")
	}
	if problems := utilvalidation.IsDNS1123Subdomain(opts.TargetStorageClass); len(problems) > 0 {
		return fmt.Errorf("invalid --target-storage-class %q: %s", opts.TargetStorageClass, strings.Join(problems, "; "))
	}
	if opts.SourceStorageClass != "" {
		if problems := utilvalidation.IsDNS1123Subdomain(opts.SourceStorageClass); len(problems) > 0 {
			return fmt.Errorf("invalid --source-storage-class %q: %s", opts.SourceStorageClass, strings.Join(problems, "; "))
		}
		if opts.SourceStorageClass == opts.TargetStorageClass {
			return errors.New("--source-storage-class and --target-storage-class must differ")
		}
	}
	if opts.AllNamespaces && opts.Namespace != "" {
		return errors.New("use either --namespace or --all-namespaces, not both")
	}
	if opts.Namespace != "" {
		if problems := utilvalidation.IsDNS1123Label(opts.Namespace); len(problems) > 0 {
			return fmt.Errorf("invalid --namespace %q: %s", opts.Namespace, strings.Join(problems, "; "))
		}
	}
	if opts.Selector != "" {
		if _, err := labels.Parse(opts.Selector); err != nil {
			return fmt.Errorf("invalid --selector: %w", err)
		}
	}
	seen := make(map[string]string, len(opts.AnnotationFilters))
	for _, filter := range opts.AnnotationFilters {
		key, value, err := parseAnnotationFilter(filter)
		if err != nil {
			return err
		}
		if previous, ok := seen[key]; ok && previous != value {
			return fmt.Errorf("conflicting --annotation filters for %q: %q and %q", key, previous, value)
		}
		seen[key] = value
	}
	return nil
}

func parseAnnotationFilter(filter string) (string, string, error) {
	key, value, ok := strings.Cut(filter, "=")
	if !ok || key == "" {
		return "", "", fmt.Errorf("invalid --annotation %q: expected key=value", filter)
	}
	if problems := utilvalidation.IsQualifiedName(key); len(problems) > 0 {
		return "", "", fmt.Errorf("invalid --annotation key %q: %s", key, strings.Join(problems, "; "))
	}
	return key, value, nil
}

func matchesAnnotationFilters(filters []string, annotations map[string]string) (bool, error) {
	for _, filter := range filters {
		key, value, err := parseAnnotationFilter(filter)
		if err != nil {
			return false, err
		}
		if annotations[key] != value {
			return false, nil
		}
	}
	return true, nil
}

func validatePVCCompatibility(volumeMode *corev1.PersistentVolumeMode, accessModes []corev1.PersistentVolumeAccessMode) error {
	if volumeMode != nil && *volumeMode == corev1.PersistentVolumeBlock {
		return errors.New("block volumeMode is not supported; scmigrate requires filesystem PVCs")
	}
	for _, mode := range accessModes {
		if mode == corev1.ReadWriteOncePod {
			return errors.New("ReadWriteOncePod access mode is not supported because sync pods need to mount the PVC during migration")
		}
	}
	return nil
}
