GO ?= $(shell command -v go 2>/dev/null || printf '%s' /usr/local/go/bin/go)
PYTHON ?= python3

.PHONY: build test check fmt run demo benchmark assets

build:
	mkdir -p bin
	$(GO) build -trimpath -o bin/ledgerd ./cmd/ledgerd
	$(GO) build -trimpath -o bin/bankctl ./cmd/bankctl

test:
	$(GO) test -race -cover ./...

check: assets
	$(GO) vet ./...
	$(GO) test -race -cover ./...

fmt:
	$(GO) fmt ./...

assets:
	$(PYTHON) scripts/check_assets.py

run: build
	$(PYTHON) scripts/cluster.py run

demo: build
	$(PYTHON) scripts/cluster.py demo

benchmark: build
	$(PYTHON) scripts/cluster.py benchmark --transactions 100 --concurrency 4
