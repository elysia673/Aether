BINARY_NAME_SERVER := aether-server
BINARY_NAME_CLIENT := aether-client
BINARY_NAME_CLI := aether-cli

BUILD_DIR := build

GO := go
GOFLAGS := -v
LDFLAGS := -s -w -extldflags '-static'
CGO_ENABLED := 0

DARWIN_GOOS := darwin
DARWIN_GOARCH := arm64

LINUX_GOOS := linux
LINUX_GOARCH := amd64

LINUX_ARM64_GOOS := linux
LINUX_ARM64_GOARCH := arm64

WINDOWS_GOOS := windows
WINDOWS_GOARCH := amd64

ENV_FILE := $(BUILD_DIR)/.env

.PHONY: all build-darwin build-linux build-windows build-linux-arm64 clean run-server run-client dev-server dev-client generate-env

all: build-darwin build-linux build-windows build-linux-arm64

# 构建 Darwin (macOS) 平台
build-darwin: | generate-env
	@echo "Building for darwin/arm64..."
	@mkdir -p $(BUILD_DIR)/$(DARWIN_GOOS)-$(DARWIN_GOARCH)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(DARWIN_GOOS) GOARCH=$(DARWIN_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(DARWIN_GOOS)-$(DARWIN_GOARCH)/$(BINARY_NAME_SERVER) \
		./Server
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(DARWIN_GOOS) GOARCH=$(DARWIN_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(DARWIN_GOOS)-$(DARWIN_GOARCH)/$(BINARY_NAME_CLIENT) \
		./Client
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(DARWIN_GOOS) GOARCH=$(DARWIN_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(DARWIN_GOOS)-$(DARWIN_GOARCH)/$(BINARY_NAME_CLI) \
		./cmd/aether-cli

# 构建 Linux amd64 平台
build-linux: | generate-env
	@echo "Building for linux/amd64..."
	@mkdir -p $(BUILD_DIR)/$(LINUX_GOOS)-$(LINUX_GOARCH)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(LINUX_GOOS) GOARCH=$(LINUX_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(LINUX_GOOS)-$(LINUX_GOARCH)/$(BINARY_NAME_SERVER) \
		./Server
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(LINUX_GOOS) GOARCH=$(LINUX_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(LINUX_GOOS)-$(LINUX_GOARCH)/$(BINARY_NAME_CLIENT) \
		./Client
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(LINUX_GOOS) GOARCH=$(LINUX_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(LINUX_GOOS)-$(LINUX_GOARCH)/$(BINARY_NAME_CLI) \
		./cmd/aether-cli

# 构建 Linux arm64 平台
build-linux-arm64: | generate-env
	@echo "Building for linux/arm64..."
	@mkdir -p $(BUILD_DIR)/$(LINUX_ARM64_GOOS)-$(LINUX_ARM64_GOARCH)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(LINUX_ARM64_GOOS) GOARCH=$(LINUX_ARM64_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(LINUX_ARM64_GOOS)-$(LINUX_ARM64_GOARCH)/$(BINARY_NAME_SERVER) \
		./Server
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(LINUX_ARM64_GOOS) GOARCH=$(LINUX_ARM64_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(LINUX_ARM64_GOOS)-$(LINUX_ARM64_GOARCH)/$(BINARY_NAME_CLIENT) \
		./Client
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(LINUX_ARM64_GOOS) GOARCH=$(LINUX_ARM64_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(LINUX_ARM64_GOOS)-$(LINUX_ARM64_GOARCH)/$(BINARY_NAME_CLI) \
		./cmd/aether-cli

# 构建 Windows 平台
build-windows: | generate-env
	@echo "Building for windows/amd64..."
	@mkdir -p $(BUILD_DIR)/$(WINDOWS_GOOS)-$(WINDOWS_GOARCH)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(WINDOWS_GOOS) GOARCH=$(WINDOWS_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(WINDOWS_GOOS)-$(WINDOWS_GOARCH)/$(BINARY_NAME_SERVER).exe \
		./Server
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(WINDOWS_GOOS) GOARCH=$(WINDOWS_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(WINDOWS_GOOS)-$(WINDOWS_GOARCH)/$(BINARY_NAME_CLIENT).exe \
		./Client
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(WINDOWS_GOOS) GOARCH=$(WINDOWS_GOARCH) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(WINDOWS_GOOS)-$(WINDOWS_GOARCH)/$(BINARY_NAME_CLI).exe \
		./cmd/aether-cli

clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)

# 内部目标：生成 build/.env 并打印内容（仅在文件不存在时生成）
generate-env:
	@mkdir -p $(BUILD_DIR)
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "AETHER_API_KEY=$$(openssl rand -base64 32)" > $(ENV_FILE); \
		echo "AETHER_CLIENT_TOKEN=$$(openssl rand -base64 16)" >> $(ENV_FILE); \
		echo "Generated new $(ENV_FILE):"; \
		cat $(ENV_FILE); \
	else \
		echo "Using existing $(ENV_FILE):"; \
		cat $(ENV_FILE); \
	fi

# 以下为调试运行目标，依赖 .env 文件
dev-server: $(ENV_FILE)
	@echo "Starting server using $(ENV_FILE)..."
	env $$(cat $(ENV_FILE) | xargs) go run ./Server

dev-client: $(ENV_FILE)
	@echo "Starting client using $(ENV_FILE)..."
	env $$(cat $(ENV_FILE) | xargs) go run ./Client

run-server: build-darwin $(ENV_FILE)
	@echo "Starting built server using $(ENV_FILE)..."
	env $$(cat $(ENV_FILE) | xargs) $(BUILD_DIR)/$(DARWIN_GOOS)-$(DARWIN_GOARCH)/$(BINARY_NAME_SERVER)

run-client: build-darwin $(ENV_FILE)
	@echo "Starting built client using $(ENV_FILE)..."
	env $$(cat $(ENV_FILE) | xargs) $(BUILD_DIR)/$(DARWIN_GOOS)-$(DARWIN_GOARCH)/$(BINARY_NAME_CLIENT)
