# cosiflow
The COSI SDOC pipeline based on Apache Airflow

1) To build the docker image:

Mac:

docker build --platform linux/arm64 -t airflow:1.0.0 -f Dockerfile .

Linux:

docker build -t airflow:1.0.0 -f Dockerfile .


2) Execute postgresql server container

docker pull postgres

docker run --name my_postgres -e POSTGRES_USER=airflow_user -e POSTGRES_PASSWORD=secure_password -e POSTGRES_DB=airflow_db -p 5432:5432 -d postgres

If you want to enter into the docker container: docker exec -it my_postgres psql -U airflow_user -d airflow_db

Take the IP number of the postgres container

docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' my_postgres


3) Execute airflow container

docker run --rm -t -d -p 8080:8080 --name airflow1 -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix:rw -v /home/cosi/cosiflow:/shared_dir  airflow:1.0.0

docker exec -it airflow1 bash

cd
. entrypoint.sh

4) Install airflow inside the docker container

Execute the following commands inside the airflow1 container. Change the path if needed:

export AIRFLOW_HOME=/shared_dir/airflow

Install airflow:

AIRFLOW_VERSION=2.10.3
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.10.3 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.8.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Note: a library is missing. Must be installed via pip

pip install psycopg2

5) Configure airflow to run with postgres

cd /shared_dir
vi airflow.cfg.postgresql

Change this line with postgres IP

sql_alchemy_conn = postgresql+psycopg2://airflow_user:secure_password@172.17.0.5/airflow_db

(change the 172.17.0.5 with the right IP)

cp airflow.cfg.postgresql $AIRFLOW_HOME/airflow.cfg


Note: use airflow.cfg.SequentialExecutor to run dags without parallism (only for development purpose). In this change nothing must be changed with respect to the standard installation or restor cp airflow.cfg.SequentialExecutor $AIRFLOW_HOME/airflow.cfg

5) Run airflow

Copy dags to $AIRFLOW_HOME/dags

Run airflow

airflow standalone

6) Connect to the web server using a browser

Login with username: admin  password: -


