.PHONY: reset

reset:
	@echo "Stopping and removing containers, images, and volumes..."
	docker compose down --rmi all --volumes

	@echo "Running airflow-init..."
	docker compose run --rm airflow-init

	@echo "Starting services..."
	docker compose up -d --build

	@echo "Done."
