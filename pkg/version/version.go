package version

import (
	"fmt"
	"io"
	"regexp"
	"runtime/debug"
	"strings"
)

const RunnerImageRepository = "ghcr.io/laverya/scmigrate-runner"

var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"

	releaseVersionPattern = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z][0-9A-Za-z.-]*)?$`)
)

func Print(out io.Writer) {
	buildVersion := Effective()
	fmt.Fprintf(out, "scmigrate %s\n", buildVersion)
	fmt.Fprintf(out, "commit: %s\n", Commit)
	fmt.Fprintf(out, "date: %s\n", Date)
	if image := DefaultRunnerImage(buildVersion); image != "" {
		fmt.Fprintf(out, "default runner image: %s\n", image)
	}
}

func Effective() string {
	buildVersion := strings.TrimSpace(Version)
	if buildVersion != "" && buildVersion != "dev" && buildVersion != "(devel)" {
		return buildVersion
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		moduleVersion := strings.TrimSpace(info.Main.Version)
		if moduleVersion != "" && moduleVersion != "(devel)" {
			return moduleVersion
		}
	}
	if buildVersion == "" || buildVersion == "(devel)" {
		return "dev"
	}
	return buildVersion
}

func DefaultRunnerImage(buildVersion string) string {
	buildVersion = strings.TrimSpace(buildVersion)
	if !releaseVersionPattern.MatchString(buildVersion) {
		return ""
	}
	return RunnerImageRepository + ":" + buildVersion
}
