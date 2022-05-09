# Create dags logs and plugins folders
mkdir -p ./dags ./logs ./plugins
mkdir -p ./dags/files
mkdir -p ./dags/graphs

# Set user
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

cp .env_postgres  dags/.env

# Init airflow db metadata
docker-compose up airflow-init

# Run airflow
docker-compose up -d

# Go to localhost:8080 with user/pass: airflow
