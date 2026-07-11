package scmigrate

import (
	"strings"
	"testing"
)

func TestValidateOptionsRejectsInvalidSelectionInput(t *testing.T) {
	tests := []struct {
		name string
		opts Options
		want string
	}{
		{
			name: "malformed annotation filter",
			opts: Options{TargetStorageClass: "fast", AnnotationFilters: []string{"approved"}},
			want: "expected key=value",
		},
		{
			name: "invalid annotation key",
			opts: Options{TargetStorageClass: "fast", AnnotationFilters: []string{"bad key=true"}},
			want: "invalid --annotation key",
		},
		{
			name: "conflicting annotation filters",
			opts: Options{TargetStorageClass: "fast", AnnotationFilters: []string{"approved=true", "approved=false"}},
			want: "conflicting --annotation filters",
		},
		{
			name: "invalid label selector",
			opts: Options{TargetStorageClass: "fast", Selector: "app in ("},
			want: "invalid --selector",
		},
		{
			name: "invalid storage class",
			opts: Options{TargetStorageClass: "Not Valid"},
			want: "invalid --target-storage-class",
		},
		{
			name: "same source and target",
			opts: Options{SourceStorageClass: "fast", TargetStorageClass: "fast"},
			want: "must differ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOptions(tt.opts)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateOptions() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestMatchesAnnotationFiltersSupportsEmptyValues(t *testing.T) {
	matched, err := matchesAnnotationFilters([]string{"example.com/approved="}, map[string]string{"example.com/approved": ""})
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatal("expected empty annotation value to match")
	}
}
