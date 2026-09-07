.PHONY: integration-test build test lint fmt clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Compile the SDK
	./gradlew build

test: ## Run all tests
	./gradlew test

lint: ## Run linting checks
	./gradlew ktlintCheck

fmt: ## Check code formatting
	./gradlew ktlintFormat

clean: ## Clean build artifacts
	./gradlew clean

package: ## Build JAR
	./gradlew jar

publish: ## Publish to Maven Local
	./gradlew publishToMavenLocal

integration-test: ## Run integration tests (requires Docker)
	docker compose -f docker-compose.test.yml up -d
	@trap 'docker compose -f docker-compose.test.yml down -v' EXIT; \
	echo "Waiting for Streamline server..."; \
	ready=false; \
	for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health/live > /dev/null 2>&1; then \
			echo "Server ready"; \
			ready=true; \
			break; \
		fi; \
		sleep 2; \
	done; \
	if [ "$$ready" != "true" ]; then \
		echo "Server failed to become healthy"; \
		exit 1; \
	fi; \
	./gradlew integrationTest
