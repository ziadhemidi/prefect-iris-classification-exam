.PHONY: up test down logs

up:
	docker compose up -d --build

test:
	docker compose --profile test run --rm test

down:
	docker compose down

logs:
	docker compose logs -f