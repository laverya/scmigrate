package version

import (
	"bytes"
	"strings"
	"testing"
)

func TestDefaultRunnerImageUsesReleaseVersion(t *testing.T) {
	for _, version := range []string{"v0.1.0", "0.1.0", "v0.1.0-rc.1"} {
		if got, want := DefaultRunnerImage(version), RunnerImageRepository+":"+version; got != want {
			t.Fatalf("DefaultRunnerImage(%q) = %q, want %q", version, got, want)
		}
	}
}

func TestDefaultRunnerImageIgnoresNonReleaseVersion(t *testing.T) {
	for _, version := range []string{"", "dev", "latest", "abc123", "v0.1", "v0.1.0+build"} {
		if got := DefaultRunnerImage(version); got != "" {
			t.Fatalf("DefaultRunnerImage(%q) = %q, want empty", version, got)
		}
	}
}

func TestPrintIncludesRunnerImageOnlyForRelease(t *testing.T) {
	oldVersion, oldCommit, oldDate := Version, Commit, Date
	t.Cleanup(func() {
		Version, Commit, Date = oldVersion, oldCommit, oldDate
	})
	Version, Commit, Date = "v0.1.0", "abc123", "2026-05-29T00:00:00Z"

	var out bytes.Buffer
	Print(&out)
	text := out.String()
	for _, want := range []string{"scmigrate v0.1.0", "commit: abc123", "date: 2026-05-29T00:00:00Z", "default runner image: " + RunnerImageRepository + ":v0.1.0"} {
		if !strings.Contains(text, want) {
			t.Fatalf("version output %q missing %q", text, want)
		}
	}

	Version = "dev"
	out.Reset()
	Print(&out)
	if strings.Contains(out.String(), "default runner image:") {
		t.Fatalf("dev version output unexpectedly included runner image: %q", out.String())
	}
}
