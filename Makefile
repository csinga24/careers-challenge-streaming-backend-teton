# Convenience targets. Python 3.10+ stdlib only, no pip dependencies.

PYTHON ?= python3
SERVICE_URL ?= http://localhost:8080

.PHONY: help example run test lint db db-stop smoke baseline burst offline adversarial

help:
	@echo "Targets:"
	@echo "  example        run the example stub service on :8080 (replace with yours)"
	@echo "  run            run our Go service (server/) on :8080"
	@echo "  test           run the Go service's unit tests (server/)"
	@echo "  lint           gofmt + go vet + staticcheck on the Go service (server/)"
	@echo "  db             start Postgres in Docker for restart correctness (M4)"
	@echo "  db-stop        stop and remove the Postgres container from 'db'"
	@echo "  smoke          30s baseline run against \$$SERVICE_URL + scorecard"
	@echo "  baseline       60s baseline"
	@echo "  burst          3min run with two 10x bursts"
	@echo "  offline        2min run with 20% of devices going offline + replaying"
	@echo "  adversarial    4min run combining burst + offline + clock skew"
	@echo
	@echo "Override SERVICE_URL=... or DEVICES=... as needed."

DEVICES ?= 50

example:
	$(PYTHON) example_solution/service.py

run:
	cd server && go run ./cmd/server

test:
	cd server && go test ./... -race

lint:
	@echo "gofmt..."
	@cd server && test -z "$$(gofmt -s -l .)" || { echo "not gofmt'd:"; gofmt -s -l .; exit 1; }
	@echo "go vet..."
	@cd server && go vet ./...
	@echo "staticcheck..."
	@if command -v staticcheck >/dev/null 2>&1; then \
		(cd server && staticcheck ./...) && echo "lint: all checks passed"; \
	else \
		echo "  staticcheck not installed, skipping (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
		echo "lint: gofmt+vet passed, staticcheck skipped (not installed)"; \
	fi

db:
	docker run -d --name teton-postgres \
		-e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=teton \
		-p 5432:5432 postgres:16-alpine
	@echo "Postgres starting on :5432. Run the service against it with:"
	@echo '  DATABASE_URL="postgres://postgres:postgres@localhost:5432/teton?sslmode=disable" make run'

db-stop:
	docker rm -f teton-postgres

smoke:
	$(PYTHON) eval/check.py smoke --target $(SERVICE_URL) --devices $(DEVICES)

baseline:
	$(PYTHON) eval/check.py baseline --target $(SERVICE_URL) --devices $(DEVICES)

burst:
	$(PYTHON) eval/check.py burst --target $(SERVICE_URL) --devices $(DEVICES)

offline:
	$(PYTHON) eval/check.py offline --target $(SERVICE_URL) --devices $(DEVICES)

adversarial:
	$(PYTHON) eval/check.py adversarial --target $(SERVICE_URL) --devices $(DEVICES)
