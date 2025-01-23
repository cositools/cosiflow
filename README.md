# cosiflow
The COSI SDOC pipeline based on Apache Airflow

We assume that the cosiflow repository is in your $HOME directory.

```
cd $HOME/cosiflow/env
```

1) To build the docker image:

Mac:
```
docker build --platform linux/arm64 -t airflow:1.0.0 -f Dockerfile .
```

Linux:
```
docker build -t airflow:1.1.0 -f Dockerfile .
```

2) Execute the docker compose to start containers

```
docker-compose up -d
```

If you want to enter into the postgre docker container: docker-compose exec cosi_postgres bash

If you want to enter into the postgre docker container: docker-compose exec cosi_airflow bash


6) Connect to the web server using a browser


localhost:8080

Note: if you use a remote server you can change the docker-compose.yaml file to use another port.

For example:

  ports:
    - "28080:8080"

then from your local pc you can forward the port in this way:

ssh -N -L 28080:localhost:28080 [user]@[remote machine]

and open the airflow webpace from your local pc at localhost:28080

Login with username: admin  password: -

The password is inside the docker-compose.yaml file and can be changed.

7) To shutdown the dockers:

```
docker-compose down -v
```

