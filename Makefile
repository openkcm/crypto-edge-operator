SHELL := /bin/zsh

GO ?= go
IMG ?= cryptoedgeoperator:dev
PKG := github.com/cryptoedge/cryptoedgeoperator

.PHONY: all build run run-pid stop tidy fmt vet generate test e2e-kind e2e-full

all: build

build:
	$(GO) build ./...

run:
	$(GO) run ./main.go

run-pid: build
	$(GO) build -o /tmp/cryptoedge-operator ./main.go
	KUBECONFIG?=$(KUBECONFIG)
	KUBECONFIG=$$KUBECONFIG /tmp/cryptoedge-operator & echo $$! > /tmp/cryptoedge-operator.pid; echo "operator started PID=$$(cat /tmp/cryptoedge-operator.pid)"

stop:
	@if [ -f /tmp/cryptoedge-operator.pid ]; then \
	  PID=$$(cat /tmp/cryptoedge-operator.pid); \
	  if kill -0 $$PID 2>/dev/null; then \
	    echo "Stopping operator PID=$$PID"; kill $$PID; \
	    for i in $$(seq 1 10); do kill -0 $$PID 2>/dev/null || break; sleep 0.3; done; \
	    if kill -0 $$PID 2>/dev/null; then echo "Force killing PID=$$PID"; kill -9 $$PID; fi; \
	  else echo "PID file present but process not running"; fi; \
	  rm -f /tmp/cryptoedge-operator.pid; \
	else echo "No PID file found"; fi

tidy:
	$(GO) mod tidy

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

generate: # placeholder for controller-gen invocation
	@echo "(TODO) controller-gen CRDs"

test:
	$(GO) test ./...

e2e-kind:
	./hack/e2e-kind.sh

e2e-full:
	bash hack/e2e-full.sh
