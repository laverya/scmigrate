GO ?= $(shell command -v go 2>/dev/null || printf /usr/local/go/bin/go)
CGO_ENABLED ?= 0
GOCACHE ?= /tmp/scmigrate-go-cache
BIN ?= $(CURDIR)/bin/kubectl-scmigrate
VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || printf unknown)
DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS ?= -X github.com/laverya/scmigrate/pkg/version.Version=$(VERSION) -X github.com/laverya/scmigrate/pkg/version.Commit=$(COMMIT) -X github.com/laverya/scmigrate/pkg/version.Date=$(DATE)

.PHONY: build test e2e

build:
	mkdir -p $(dir $(BIN))
	CGO_ENABLED=$(CGO_ENABLED) GOCACHE=$(GOCACHE) $(GO) build -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/kubectl-scmigrate

test:
	GOCACHE=$(GOCACHE) $(GO) test ./...

e2e: build
	GO=$(GO) GOCACHE=$(GOCACHE) SCMIGRATE_BIN=$(BIN) ./hack/e2e-kind.sh
