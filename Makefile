.PHONY: up down producer spark test dbt-run dbt-test lint

up:
	docker-compose up -d
	@echo "Kafka UI: http://localhost:8080"
	@echo "Prometheus: http://localhost:9090"

down:
	docker-compose down

producer:
	python src/producer/transaction_producer.py

bad-producer:
	python src/producer/bad_event_simulator.py

spark:
	python src/spark/streaming_job.py

replay-dlq:
	python src/replay/replay_dlq.py

metrics:
	python src/monitoring/metrics_exporter.py

test:
	pytest tests/ -v --tb=short

lint:
	ruff check . --ignore E501

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve
