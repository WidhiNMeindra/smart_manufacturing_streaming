# Makefile for managing the Docker environment
# Final corrected version with proper TAB characters.

.PHONY: all up down logs ps jupyter spark kafka postgres metabase minio

# --- Primary Controls ---
# Start all services in detached mode
all:
	@echo "Starting all services in the background..."
	docker-compose up -d --build

# 'up' is an alias for 'all'
up: all

# Stop and remove all services, containers, networks, and volumes
down:
	@echo "Stopping all services and removing containers, networks, and volumes..."
	docker-compose down -v

# --- Individual Service Controls ---
# Each command starts the specific service(s) and opens the UI if available.

# Start Jupyter and open its UI
jupyter:
	@echo "Starting Jupyter service..."
	docker-compose up -d jupyter
	@echo "Waiting for the service to initialize..."
	@sleep 5
	@echo "Opening Jupyter Notebook UI at http://localhost:8888"
	@open http://localhost:8888 || start http://localhost:8888 || xdg-open http://localhost:8888

# Start Spark cluster and open Master UI
spark:
	@echo "Starting Spark Master and Worker services..."
	docker-compose up -d spark-master spark-worker
	@echo "Waiting for services to initialize..."
	@sleep 5
	@echo "Opening Spark Master UI at http://localhost:8080"
	@open http://localhost:8080 || start http://localhost:8080 || xdg-open http://localhost:8080

# Start Kafka cluster
kafka:
	@echo "Starting Kafka and Zookeeper services..."
	docker-compose up -d kafka
	@echo "Kafka and Zookeeper are starting in the background."

# Start PostgreSQL service
postgres:
	@echo "Starting PostgreSQL service..."
	docker-compose up -d postgres
	@echo "PostgreSQL is starting in the background. You can connect on port 5432."

# Start Metabase and open its UI
metabase:
	@echo "Starting Metabase service..."
	docker-compose up -d metabase
	@echo "Waiting for Metabase to be ready (this may take a moment)..."
	@sleep 20
	@echo "Opening Metabase UI at http://localhost:3000"
	@open http://localhost:3000 || start http://localhost:3000 || xdg-open http://localhost:3000

# Start MinIO and open its UI
minio:
	@echo "Starting MinIO service..."
	docker-compose up -d minio
	@echo "Waiting for the service to initialize..."
	@sleep 5
	@echo "Opening MinIO Console UI at http://localhost:9001"
	@open http://localhost:9001 || start http://localhost:9001 || xdg-open http://localhost:9001


# --- Utility Commands ---
# Follow logs from all services
logs:
	@echo "Tailing logs from all services..."
	docker-compose logs -f

# List running services
ps:
	@echo "Listing running services..."
	docker-compose ps
