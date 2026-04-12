BINARY_NAME_SERVER := aether-server
BINARY_NAME_CLIENT := aether-client

BUILD_DIR := build

GO := go
GOFLAGS := -v

LOCAL_GOOS := $(shell go env GOOS)
LOCAL_GOARCH := $(shell go env GOARCH)

LINUX_AMD64_GOOS := linux
LINUX_AMD64_GOARCH := amd64

.PHONY: all build-local build-linux clean

all: build-local build-linux

build-local:
	@echo "Building for local ($(LOCAL_GOOS)/$(LOCAL_GOARCH))..."
	@mkdir -p $(BUILD_DIR)/$(LOCAL_GOOS)-$(LOCAL_GOARCH)
	GOOS=$(LOCAL_GOOS) GOARCH=$(LOCAL_GOARCH) $(GO) build $(GOFLAGS) \
		-o $(BUILD_DIR)/$(LOCAL_GOOS)-$(LOCAL_GOARCH)/$(BINARY_NAME_SERVER) \
		./Server
	GOOS=$(LOCAL_GOOS) GOARCH=$(LOCAL_GOARCH) $(GO) build $(GOFLAGS) \
		-o $(BUILD_DIR)/$(LOCAL_GOOS)-$(LOCAL_GOARCH)/$(BINARY_NAME_CLIENT) \
		./Client

build-linux:
	@echo "Building for linux/amd64..."
	@mkdir -p $(BUILD_DIR)/$(LINUX_AMD64_GOOS)-$(LINUX_AMD64_GOARCH)
	GOOS=$(LINUX_AMD64_GOOS) GOARCH=$(LINUX_AMD64_GOARCH) $(GO) build $(GOFLAGS) \
		-o $(BUILD_DIR)/$(LINUX_AMD64_GOOS)-$(LINUX_AMD64_GOARCH)/$(BINARY_NAME_SERVER) \
		./Server
	GOOS=$(LINUX_AMD64_GOOS) GOARCH=$(LINUX_AMD64_GOARCH) $(GO) build $(GOFLAGS) \
		-o $(BUILD_DIR)/$(LINUX_AMD64_GOOS)-$(LINUX_AMD64_GOARCH)/$(BINARY_NAME_CLIENT) \
		./Client

clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
