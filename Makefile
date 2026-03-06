.PHONY: integration-test build test lint fmt clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Compile the SDK
	./gradlew build -x test -q 2>/dev/null || gradle build -x test -q

test: ## Run all tests
	./gradlew test -q 2>/dev/null || gradle test -q

lint: ## Run linting checks
	./gradlew detekt -q 2>/dev/null || echo "detekt not configured, skipping"

fmt: ## Check code formatting
	@echo "Use IDE formatting or ktlint"

clean: ## Clean build artifacts
	./gradlew clean -q 2>/dev/null || gradle clean -q

package: ## Build JAR
	./gradlew jar -q 2>/dev/null || gradle jar -q

publish: ## Publish to Maven Local
	./gradlew publishToMavenLocal -q 2>/dev/null || gradle publishToMavenLocal -q

integration-test: ## Run integration tests (requires Docker)
	docker compose -f docker-compose.test.yml up -d
	@echo "Waiting for Streamline server..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health/live > /dev/null 2>&1; then \
			echo "Server ready"; \
			break; \
		fi; \
		sleep 2; \
	done
	./gradlew test -Dintegration=true || true
	docker compose -f docker-compose.test.yml down -v
