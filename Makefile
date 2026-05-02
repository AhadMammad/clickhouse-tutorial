# ==============================================================================
# ClickHouse Fundamentals — Makefile
# ==============================================================================

# Load base config, then per-user overrides (local overrides base)
# These exports make shell vars available to `wait`, `sql`, `reset-db` recipes.
-include .env
-include .env.local
export

# Variables
PYTHON      := .venv/bin/python
PIP         := .venv/bin/pip
ENV_FILE    := $(if $(wildcard .env.local),.env.local,.env)
COMPOSE     := docker compose --env-file $(ENV_FILE)
PROJECT     := clickhouse-fundamentals
PYTHON_VER  := $(shell grep 'requires-python' python/pyproject.toml | grep -oE '3\.[0-9]+')

# Colors
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
RESET  := \033[0m

# Default target
.DEFAULT_GOAL := help

# Declare all non-file targets as phony
.PHONY: help up down restart logs ps wait \
        setup reset-db sql \
        install generate report demo \
        build run run-setup run-generate run-report shell docker-demo \
        run-pg-generate run-pg-report run-export-parquet run-export-raw run-raw-to-silver run-star-setup run-star-load docker-pipeline-demo \
        pg-up pg-migrate pg-generate pg-report pg-sql \
        hdfs-up hdfs-wait hdfs-ls \
        export-parquet export-raw raw-to-silver star-setup star-load full-up full-demo \
        lint format typecheck test check \
        env init-user clean clean-all docs

##@ Infrastructure

up: ## Start ClickHouse via Docker Compose
	@echo "$(GREEN)Starting ClickHouse...$(RESET)"
	$(COMPOSE) up -d clickhouse

down: ## Stop and remove containers
	@echo "$(YELLOW)Stopping containers...$(RESET)"
	$(COMPOSE) down

restart: down up ## Restart containers

logs: ## Tail ClickHouse container logs
	$(COMPOSE) logs -f clickhouse

ps: ## Show running containers
	$(COMPOSE) ps

wait: ## Wait until ClickHouse is healthy (60s timeout)
	@echo "$(YELLOW)Waiting for ClickHouse to be healthy...$(RESET)"
	@elapsed=0; \
	while [ $$elapsed -lt 60 ]; do \
		if curl -s http://localhost:$${CH_HTTP_PORT:-8123}/ping > /dev/null 2>&1; then \
			echo ""; \
			echo "$(GREEN)ClickHouse is ready!$(RESET)"; \
			exit 0; \
		fi; \
		printf "."; \
		sleep 2; \
		elapsed=$$((elapsed + 2)); \
	done; \
	echo ""; \
	echo "$(RED)Timeout: ClickHouse did not become healthy within 60 seconds$(RESET)"; \
	exit 1

##@ Database

setup: ## Apply all SQL schemas
	@echo "$(GREEN)Setting up database schema...$(RESET)"
	$(PYTHON) python/main.py setup

reset-db: ## Drop and recreate database (prompts confirmation)
	@echo "$(RED)WARNING: This will drop and recreate the database!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		echo "$(YELLOW)Dropping and recreating database...$(RESET)"; \
		$(COMPOSE) exec clickhouse clickhouse-client \
			--user $$CLICKHOUSE_USER \
			--password $$CLICKHOUSE_PASSWORD \
			--query "DROP DATABASE IF EXISTS $$CLICKHOUSE_DATABASE"; \
		$(COMPOSE) exec clickhouse clickhouse-client \
			--user $$CLICKHOUSE_USER \
			--password $$CLICKHOUSE_PASSWORD \
			--query "CREATE DATABASE IF NOT EXISTS $$CLICKHOUSE_DATABASE"; \
		echo "$(GREEN)Database reset complete.$(RESET)"; \
	else \
		echo "$(YELLOW)Aborted.$(RESET)"; \
	fi

sql: ## Open ClickHouse interactive shell
	$(COMPOSE) exec clickhouse clickhouse-client \
		--user $$CLICKHOUSE_USER \
		--password $$CLICKHOUSE_PASSWORD

##@ Local Python App

install: ## Create .venv and install Python dependencies (runtime + dev)
	@echo "$(GREEN)Creating virtual environment and installing dependencies...$(RESET)"
	python$(PYTHON_VER) -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install "python/[dev]"
	@echo "$(GREEN)Installation complete.$(RESET)"

generate: ## Insert 100k sample transactions (local)
	@echo "$(GREEN)Generating sample data...$(RESET)"
	$(PYTHON) python/main.py generate --rows 100000

report: ## Print analytics reports (local)
	@echo "$(GREEN)Running analytics report...$(RESET)"
	$(PYTHON) python/main.py report

demo: env up wait install setup generate report ## Full local end-to-end demo
	@echo ""
	@echo "$(GREEN)============================================================$(RESET)"
	@echo "$(GREEN)  LOCAL DEMO COMPLETE!$(RESET)"
	@echo "$(GREEN)============================================================$(RESET)"
	@echo ""

##@ Docker App

build: ## Build the Python app Docker image
	@echo "$(GREEN)Building app image...$(RESET)"
	$(COMPOSE) build app

run: ## Run app container with default command
	$(COMPOSE) --profile app run --rm app

run-setup: ## Run schema setup inside container
	@echo "$(GREEN)Running setup in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py setup

run-generate: ## Run data generator inside container
	@echo "$(GREEN)Running data generation in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py generate --rows 100000

run-report: ## Run analytics report inside container
	@echo "$(GREEN)Running report in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py report

shell: ## Open bash shell inside app container
	$(COMPOSE) --profile app run --rm app bash

run-pg-generate: ## Generate mobile app data and write to PostgreSQL (in container)
	@echo "$(GREEN)Running pg-generate in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py pg-generate

run-pg-report: ## Print mobile app analytics from PostgreSQL (in container)
	@echo "$(GREEN)Running pg-report in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py pg-report

run-export-parquet: ## [legacy] Export PG to flat HDFS Parquet (in container)
	@echo "$(GREEN)Running export-parquet in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py export-parquet --days 30

run-export-raw: ## Export each PG table as raw HDFS Parquet (in container)
	@echo "$(GREEN)Running export-raw in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py export-raw --days 30

run-raw-to-silver: ## Join raw HDFS tables into silver layer (in container)
	@echo "$(GREEN)Running raw-to-silver in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py raw-to-silver --days 30

run-star-setup: ## Create ClickHouse star schema tables (in container)
	@echo "$(GREEN)Running star-setup in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py star-setup

run-star-load: ## Load HDFS Parquet into ClickHouse star schema (in container)
	@echo "$(GREEN)Running star-load in container...$(RESET)"
	$(COMPOSE) --profile app run --rm app python main.py star-load --days 30

docker-demo: env build up wait run-setup run-generate run-report ## Full containerized ClickHouse fundamentals demo
	@echo ""
	@echo "$(GREEN)============================================================$(RESET)"
	@echo "$(GREEN)  DOCKER DEMO COMPLETE!$(RESET)"
	@echo "$(GREEN)============================================================$(RESET)"
	@echo ""

docker-pipeline-demo: env ## Full containerized pipeline demo (PG → raw → silver → ClickHouse star)
	@echo "$(YELLOW)Tearing down existing containers and volumes (clean start)...$(RESET)"
	-$(COMPOSE) down -v
	@$(MAKE) --no-print-directory build full-up wait hdfs-wait pg-migrate run-pg-generate run-export-raw run-raw-to-silver run-star-setup run-star-load
	@echo ""
	@echo "$(GREEN)============================================================$(RESET)"
	@echo "$(GREEN)  DOCKER PIPELINE DEMO COMPLETE!$(RESET)"
	@echo "$(GREEN)  PostgreSQL → HDFS raw → HDFS silver → ClickHouse Star$(RESET)"
	@echo "$(GREEN)============================================================$(RESET)"
	@echo ""

##@ PostgreSQL (Mobile App Interaction Data)

pg-up: ## Start PostgreSQL container
	@echo "$(GREEN)Starting PostgreSQL...$(RESET)"
	$(COMPOSE) up -d postgres

pg-migrate: ## Run Liquibase migrations (creates 3NF tables in PostgreSQL)
	@echo "$(GREEN)Running Liquibase migrations...$(RESET)"
	$(COMPOSE) --profile migrate run --rm --no-deps liquibase
	@echo "$(GREEN)Migrations complete.$(RESET)"

pg-generate: ## Generate mobile app interaction data and write to PostgreSQL
	@echo "$(GREEN)Generating mobile app interaction data...$(RESET)"
	$(PYTHON) python/main.py pg-generate

pg-report: ## Print mobile app analytics from PostgreSQL
	@echo "$(GREEN)Printing PostgreSQL analytics...$(RESET)"
	$(PYTHON) python/main.py pg-report

pg-sql: ## Open PostgreSQL interactive shell
	$(COMPOSE) exec postgres psql -U $${PG_USER:-pguser} -d $${PG_DATABASE:-appdb}

##@ HDFS

hdfs-up: ## Start HDFS (namenode + datanode)
	@echo "$(GREEN)Starting HDFS...$(RESET)"
	$(COMPOSE) up -d namenode datanode

hdfs-wait: ## Wait until HDFS NameNode is healthy (60s timeout)
	@echo "$(YELLOW)Waiting for HDFS to be ready...$(RESET)"
	@elapsed=0; \
	while [ $$elapsed -lt 60 ]; do \
		if curl -s http://localhost:$${HDFS_PORT:-9870} > /dev/null 2>&1; then \
			echo "$(GREEN)HDFS is ready!$(RESET)"; \
			exit 0; \
		fi; \
		printf "."; \
		sleep 3; \
		elapsed=$$((elapsed + 3)); \
	done; \
	echo "$(RED)Timeout: HDFS did not become healthy$(RESET)"; \
	exit 1

hdfs-ls: ## List HDFS app_interactions directory
	@curl -s "http://localhost:$${HDFS_PORT:-9870}/webhdfs/v1$${HDFS_BASE_PATH:-/data/app_interactions}?op=LISTSTATUS" \
		| python3 -m json.tool 2>/dev/null || echo "Directory does not exist yet."

##@ ETL Pipeline

export-parquet: ## [legacy] Export PG to flat HDFS Parquet (last 30 days)
	@echo "$(GREEN)Exporting Parquet files to HDFS...$(RESET)"
	$(PYTHON) python/main.py export-parquet --days 30

export-raw: ## Export each PG table as raw HDFS Parquet (last 30 days)
	@echo "$(GREEN)Exporting raw tables to HDFS...$(RESET)"
	$(PYTHON) python/main.py export-raw --days 30

raw-to-silver: ## Join raw HDFS tables into silver layer (last 30 days)
	@echo "$(GREEN)Transforming raw → silver on HDFS...$(RESET)"
	$(PYTHON) python/main.py raw-to-silver --days 30

star-setup: ## Create ClickHouse star schema tables
	@echo "$(GREEN)Creating ClickHouse star schema...$(RESET)"
	$(PYTHON) python/main.py star-setup

star-load: ## Load HDFS Parquet into ClickHouse star schema (last 30 days)
	@echo "$(GREEN)Loading star schema from HDFS Parquet...$(RESET)"
	$(PYTHON) python/main.py star-load --days 30

full-up: ## Start all services (ClickHouse + PostgreSQL + HDFS)
	@echo "$(GREEN)Starting all services...$(RESET)"
	$(COMPOSE) up -d clickhouse postgres namenode datanode

full-demo: env build full-up wait hdfs-wait pg-migrate run-pg-generate run-export-raw run-raw-to-silver run-star-setup run-star-load ## Full pipeline demo (PG → raw → silver → ClickHouse star), all in containers
	@echo ""
	@echo "$(GREEN)============================================================$(RESET)"
	@echo "$(GREEN)  FULL PIPELINE DEMO COMPLETE!$(RESET)"
	@echo "$(GREEN)  PostgreSQL → HDFS raw → HDFS silver → ClickHouse Star$(RESET)"
	@echo "$(GREEN)============================================================$(RESET)"
	@echo ""

##@ Development

lint: ## Lint with ruff (config in pyproject.toml)
	@echo "$(GREEN)Linting...$(RESET)"
	.venv/bin/ruff check python/

format: ## Format with ruff (config in pyproject.toml)
	@echo "$(GREEN)Formatting...$(RESET)"
	.venv/bin/ruff format python/

typecheck: ## Type-check with mypy (config in pyproject.toml)
	@echo "$(GREEN)Type-checking...$(RESET)"
	.venv/bin/mypy python/ --config-file python/pyproject.toml

test: ## Run pytest
	@echo "$(GREEN)Running tests...$(RESET)"
	@if [ -d "python/tests/" ]; then \
		.venv/bin/pytest python/tests/ -v; \
	else \
		echo "$(YELLOW)No python/tests/ directory found$(RESET)"; \
	fi

check: lint typecheck ## lint + typecheck

##@ Utilities

env: ## Create .env from .env.example
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(GREEN)Created .env from .env.example$(RESET)"; \
	else \
		echo "$(YELLOW).env already exists, skipping$(RESET)"; \
	fi

init-user: ## Interactive setup: prompt for config values, auto-detect available ports
	@if [ -f .env.local ]; then \
		echo "$(YELLOW).env.local already exists. To regenerate:$(RESET)"; \
		echo "  rm .env.local && make init-user"; \
	else \
		echo ""; \
		echo "$(GREEN)Personal environment setup$(RESET)"; \
		echo "-------------------------------------------"; \
		read -p "  Project name          [$$USER]: " project; \
		project=$${project:-$$USER}; \
		read -p "  ClickHouse password   [test]: " ch_pass; \
		ch_pass=$${ch_pass:-test}; \
		read -p "  ClickHouse database   [default]: " ch_db; \
		ch_db=$${ch_db:-default}; \
		read -p "  PostgreSQL password   [pgpassword]: " pg_pass; \
		pg_pass=$${pg_pass:-pgpassword}; \
		read -p "  PostgreSQL database   [appdb]: " pg_db; \
		pg_db=$${pg_db:-appdb}; \
		echo ""; \
		echo "$(YELLOW)Scanning for available ports...$(RESET)"; \
		http_port=8123; \
		while nc -z localhost $$http_port 2>/dev/null; do http_port=$$((http_port + 1)); done; \
		native_port=9000; \
		while nc -z localhost $$native_port 2>/dev/null; do native_port=$$((native_port + 1)); done; \
		pg_port=5432; \
		while nc -z localhost $$pg_port 2>/dev/null; do pg_port=$$((pg_port + 1)); done; \
		hdfs_port=9870; \
		while nc -z localhost $$hdfs_port 2>/dev/null; do hdfs_port=$$((hdfs_port + 1)); done; \
		printf '%s\n' \
			"COMPOSE_PROJECT_NAME=$${project}_clickhouse" \
			"" \
			"# ClickHouse" \
			"CH_HTTP_PORT=$$http_port" \
			"CH_NATIVE_PORT=$$native_port" \
			"CH_DATA_DIR=./clickhouse-data-$$project" \
			"CLICKHOUSE_HOST=localhost" \
			"CLICKHOUSE_PORT=$$http_port" \
			"CLICKHOUSE_USER=default" \
			"CLICKHOUSE_PASSWORD=$$ch_pass" \
			"CLICKHOUSE_DATABASE=$$ch_db" \
			"CLICKHOUSE_DB=$$ch_db" \
			"" \
			"# PostgreSQL" \
			"PG_HOST=localhost" \
			"PG_PORT=$$pg_port" \
			"PG_USER=pguser" \
			"PG_PASSWORD=$$pg_pass" \
			"PG_DATABASE=$$pg_db" \
			"" \
			"# HDFS (WebHDFS REST API)" \
			"HDFS_HOST=localhost" \
			"HDFS_PORT=$$hdfs_port" \
			"HDFS_USER=root" \
			"HDFS_BASE_PATH=/data/app_interactions" \
			> .env.local; \
		echo ""; \
		echo "$(GREEN)Created .env.local$(RESET)"; \
		printf "  %-28s %s\n" "COMPOSE_PROJECT_NAME" "$${project}_clickhouse"; \
		printf "  %-28s %s\n" "CH_HTTP_PORT" "$$http_port"; \
		printf "  %-28s %s\n" "CH_NATIVE_PORT" "$$native_port"; \
		printf "  %-28s %s\n" "CH_DATA_DIR" "./clickhouse-data-$$project"; \
		printf "  %-28s %s\n" "CLICKHOUSE_DATABASE" "$$ch_db"; \
		printf "  %-28s %s\n" "PG_PORT" "$$pg_port"; \
		printf "  %-28s %s\n" "PG_DATABASE" "$$pg_db"; \
		printf "  %-28s %s\n" "HDFS_PORT" "$$hdfs_port"; \
		echo ""; \
	fi

clean: ## Remove caches, .venv, and compiled files
	@echo "$(YELLOW)Cleaning up...$(RESET)"
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(GREEN)Clean complete.$(RESET)"

clean-all: ## Stop containers, remove volumes, and clean all Python caches (keeps Docker image)
	@echo "$(RED)Stopping containers and removing volumes...$(RESET)"
	$(COMPOSE) down -v
	sudo rm -rf $${CH_DATA_DIR:-./clickhouse-data}
	@echo "$(YELLOW)Cleaning Python caches...$(RESET)"
	rm -rf .venv
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(GREEN)clean-all complete. Docker image preserved.$(RESET)"

docs: ## Show docs index
	@echo "$(GREEN)Documentation Index$(RESET)"
	@echo "===================="
	@if [ -d "docs/" ]; then \
		for f in docs/*.md; do \
			if [ -f "$$f" ]; then \
				heading=$$(head -20 "$$f" | grep -m1 "^#" | sed 's/^#* *//'); \
				echo "  $$f: $$heading"; \
			fi; \
		done; \
	else \
		echo "$(YELLOW)No docs/ directory found$(RESET)"; \
	fi

help: ## Show this help message
	@echo ""
	@echo "$(GREEN)ClickHouse Fundamentals — Makefile$(RESET)"
	@awk 'BEGIN {FS = ":.*##"} \
		/^##@ / { printf "\n$(YELLOW)%s$(RESET)\n", substr($$0, 5) } \
		/^[a-zA-Z_-]+:.*## / { printf "  make %-18s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
