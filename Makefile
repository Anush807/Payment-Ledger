# Everything runs through Docker Compose; there is no Kubernetes in this project.
SHELL := /usr/bin/env bash
COMPOSE := docker compose -f infra/docker-compose.yml
API_KEY ?= dev-key
export API_KEY

.PHONY: help build test up down restart ps logs connectors verify load clean chaos-all $(addprefix chaos-,1 2 3 4 5 6 7 8 9)

help: ## show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

build: ## compile and package every module
	mvn -B -DskipTests package

test: ## run every test (needs Docker for the Testcontainers integration tests)
	@DOCKER_HOST="$${DOCKER_HOST:-$$(test -S $$HOME/.docker/run/docker.sock && echo unix://$$HOME/.docker/run/docker.sock)}" \
	  mvn -B test

up: ## start the whole stack and wait until it is healthy
	$(COMPOSE) up -d --build
	@echo "waiting for services to report UP..."
	@for port in 8081 8082 8083 8084 8085; do \
	  for i in $$(seq 1 90); do \
	    curl -fsS http://localhost:$$port/actuator/health 2>/dev/null | grep -q '"status":"UP"' && break; \
	    sleep 2; \
	  done; \
	  curl -fsS http://localhost:$$port/actuator/health | grep -q '"status":"UP"' \
	    && echo "  port $$port UP" || { echo "  port $$port NOT UP"; exit 1; }; \
	done
	@$(MAKE) connectors

down: ## stop the stack and delete its volumes
	$(COMPOSE) down -v

restart: down up ## full restart

ps: ## container status
	$(COMPOSE) ps

logs: ## tail logs of the five services
	$(COMPOSE) logs -f order-service wallet-service payment-service ledger-service notification-service

connectors: ## (re)register the Debezium outbox connectors
	CONNECT=http://localhost:8083 infra/debezium/register.sh || \
	  $(COMPOSE) run --rm connect-init

verify: ## check all four invariants through the APIs
	chaos/verify.sh

chaos-1: ; chaos/1-kill-wallet-service.sh
chaos-2: ; chaos/2-payment-service-down.sh
chaos-3: ; chaos/3-connect-down.sh
chaos-4: ; chaos/4-kill-kafka-broker.sh
chaos-5: ; chaos/5-ledger-service-down.sh
chaos-6: ; chaos/6-full-replay.sh
chaos-7: ; chaos/7-poison-message.sh
chaos-8: ; chaos/8-flaky-notifications.sh
chaos-9: ; chaos/9-mongo-stepdown.sh

chaos-all: ## run every chaos scenario in order
	@for n in 1 2 3 4 5 6 7 8 9; do $(MAKE) chaos-$$n || exit 1; done

load: ## k6 load test (needs k6 installed)
	k6 run load-test/orders.js

clean: ## remove build output
	mvn -B -q clean
