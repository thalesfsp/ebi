###
# Params.
###

PROJECT_NAME := "ebi"
BIN_NAME := $(PROJECT_NAME)
BIN_DIR := bin
BIN_PATH := $(BIN_DIR)/$(BIN_NAME)

HAS_GODOC := $(shell command -v godoc;)
HAS_GOLANGCI := $(shell command -v golangci-lint;)

GOLANGCI_VERSION := v1.61.0

default: ci

###
# Entries.
###

build:
	@go build -o $(BIN_PATH) && echo "Build OK"

build-dev:
	@go build -gcflags="all=-N -l" -o $(BIN_PATH) && echo "Build OK"

ci: build lint

doc:
ifndef HAS_GODOC
	@echo "Could not find godoc, installing it"
	@go install golang.org/x/tools/cmd/godoc@latest
endif
	@echo "Open localhost:6060/pkg/github.com/thalesfsp/$(PROJECT_NAME)/ in your browser\n"
	@godoc -http :6060

lint:
ifndef HAS_GOLANGCI
	@echo "Could not find golangci-list, installing it"
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_VERSION)
endif
	@golangci-lint run -v -c .golangci.yml && echo "Lint OK"

test:
	@VENDOR_ENVIRONMENT="testing" go test -timeout 30s -short -v -race -cover \
	-coverprofile=coverage.out ./... && echo "Test OK"

.PHONY: build \
	build-dev \
	ci \
	doc \
	lint \
	test
