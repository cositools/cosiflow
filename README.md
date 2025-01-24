# cosiflow
The COSI SDOC pipeline based on Apache Airflow

## Build the cosiflow docker
We assume that the cosiflow repository is in your $HOME directory.

```
cd $HOME/cosiflow/env
```

Mac:
```
docker build --platform linux/arm64 -t airflow:1.0.0 -f Dockerfile .
```

Linux:
```
docker build -t airflow:1.1.0 -f Dockerfile .
```

## Execute the docker compose to start containers

```
docker compose up -d
```

If you want to enter into the postgre docker container: docker-compose exec postgres bash

If you want to enter into the postgre docker container: docker-compose exec airflow bash


## Connect to the web server using a browser


localhost:8080

Note: if you use a remote server you can change the docker-compose.yaml file to use another port.

For example:

  ports:
    - "28080:8080"

then from your local pc you can forward the port in this way:

ssh -N -L 28080:localhost:28080 [user]@[remote machine]

and open the airflow webpace from your local pc at localhost:28080

Login with username: admin  password: -

To obtain the password execute this command after the initialization of the containers

```
docker compose logs | grep pass
```

### Shutdown the dockers:

```
docker compose down -v
```


## Test the cosipy DAG

First download the data file from wasabi.

```
cd /shared_dir/pipeline
source activate cosipy
python initialize_pipeline.py
```
This script downloads the input file from wasabi and move it in /home/gamma/workspace/data

Now we must activate the DAG named "cosipt_test_v0" from the airflow website

Then we have to copy the file in the input directory to trigger the DAG

```
cd /home/gamma/workspace/data
cp GalacticScan.inc1.id1.crab2hr.extracted.tra.gz input
```
We should see that the DAG started to process the data.

This directory /home/gamma/workspace/heasarc/dl0 contains several folders with this format 2025-01-24_14-31-56.

Inside the folder we have the results of the analysis.