# Cosiflow

The COSI SDOC pipeline based on Apache Airflow

## Build the cosiflow docker

We assume that the cosiflow repository is in your $HOME directory.

```bash
cd $HOME/cosiflow/env
```

Mac:

```bash
docker build --platform linux/arm64 -t airflow:1.0.0 -f Dockerfile .
```

Linux:

```bash
docker build -t airflow:1.1.0 -f Dockerfile .
```

## Execute the docker compose to start containers

```bash
docker compose up -d
```

If you want to enter into the postgre docker container: `docker compose exec postgres bash`

If you want to enter into the postgre docker container: `docker compose exec airflow bash`

## Connect to the web server using a browser

localhost:8080

Note: if you use a remote server you can change the `docker-compose.yaml` file to use another port.

For example:
  
  ```yaml
  ports:
    - "28080:8080"
  ```

then from your local pc you can forward the port in this way:

```bash
ssh -N -L 28080:localhost:28080 [user]@[remote machine]
```

and open the airflow webpace from your local pc at `localhost:28080`

Login with username: `admin`  password: `<password>`

To obtain the password `<password>` execute this command after the initialization of the containers

```bash
docker compose logs | grep pass
```

### Shutdown the dockers

```bash
docker compose down -v
```

## Test the cosipy DAG

Enter in the docker airflow

```bash
docker compose exec airflow bash
```

First download the data file from wasabi.

```bash
cd /shared_dir/pipeline
source activate cosipy
python initialize_pipeline.py
```

This script downloads the input file from wasabi and move it in `/home/gamma/workspace/data`

Now we must activate the DAG named `"cosipt_test_v0"` from the airflow website

Then we have to copy the file in the input directory to trigger the DAG

```bash
cd /home/gamma/workspace/data
cp GalacticScan.inc1.id1.crab2hr.extracted.tra.gz input
```

We should see that the DAG started to process the data.

This directory `/home/gamma/workspace/heasarc/dl0` contains several folders with this format `2025-01-24_14-31-56`.

Inside the folder we have the results of the analysis.
