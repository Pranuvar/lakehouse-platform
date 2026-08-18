.PHONY: up down orchestration-up orchestration-down seed ps logs venv clean

up: ## bring up the four sources (postgres, minio, redpanda, mock-api)
	docker compose up -d

down: ## stop everything, keep volumes
	docker compose --profile orchestration down

orchestration-up: ## add Airflow (builds the custom image on first run)
	docker compose --profile orchestration up -d --build

orchestration-down:
	docker compose --profile orchestration stop airflow-webserver airflow-scheduler

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

venv: ## create/refresh the host venv used by seeders/
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip -q
	.venv/bin/pip install -r requirements-seed.txt

seed: ## run all seeders in dependency order (kafka seeder joins to postgres orders)
	.venv/bin/python seeders/seed_postgres_oltp.py
	.venv/bin/python seeders/seed_flatfiles.py
	.venv/bin/python seeders/seed_kafka_events.py

clean: ## tear down everything INCLUDING volumes (drops all seeded data)
	docker compose --profile orchestration down -v
