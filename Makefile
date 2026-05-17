GO ?= $(shell command -v go 2>/dev/null || printf /usr/local/go/bin/go)
GOCACHE ?= /tmp/scmigrate-go-cache
BIN ?= $(CURDIR)/bin/kubectl-scmigrate

.PHONY: build test e2e

build:
	mkdir -p $(dir $(BIN))
	GOCACHE=$(GOCACHE) $(GO) build -o $(BIN) ./cmd/kubectl-scmigrate

test:
	GOCACHE=$(GOCACHE) $(GO) test ./...

e2e: build
	GO=$(GO) GOCACHE=$(GOCACHE) SCMIGRATE_BIN=$(BIN) ./hack/e2e-kind.sh
