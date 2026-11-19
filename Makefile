PROJECT_NAME	= ton_payment
PROJECT_VERSION	= $(shell poetry version -s)
PACKAGE_NAME	= ton_payment

lint:
	poetry run ruff check --fix .
	poetry run mypy ${PACKAGE_NAME}

fmt:
	poetry run ruff format .

check:
	poetry run ruff check .
	poetry run mypy -p $(PACKAGE_NAME)

# локальный запуск отдельных процессов (БД и RabbitMQ должны быть подняты)
dev:
	POSTGRES_HOST=localhost poetry run uvicorn ton_payment.main_api:ton_payment --reload --port 8080

dev-outbox:
	POSTGRES_HOST=localhost poetry run python -m ton_payment.outbox_worker

dev-tracker:
	POSTGRES_HOST=localhost poetry run python -m ton_payment.tracker_worker

dev-broker:
	POSTGRES_HOST=localhost poetry run faststream run ton_payment.main_broker:faststream_ton_payment --reload

migration:
	@test -n "$(m)" || (echo 'usage: make migration m="message"'; exit 1)
	POSTGRES_HOST=localhost poetry run alembic revision --autogenerate -m "$(m)"

migrate:
	POSTGRES_HOST=localhost poetry run alembic upgrade head

downgrade:
	POSTGRES_HOST=localhost poetry run alembic downgrade -1

down:
	docker compose down

build:
	docker compose build

up:
	docker compose up

run: down fmt build up