# cosiflow

The COSI SDOC pipeline based on Apache Airflow

## Build the cosiflow docker

We assume that the cosiflow repository is in your $HOME directory.

```bash
cd $HOME/cosiflow/env
```

Mac:

```bash
WIP
```

Linux:

```bash
docker build
```

## Execute the docker compose to start containers

Before running the following command to start the containers, make sure to create a `.env` file with the following structure in `cosiflow/env`:

```env
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_EMAIL=admin@localhost
AIRFLOW_ADMIN_PASSWORD=<AIRFLOW_PASS>
```

Replace `<AIRFLOW_PASS>` with your desired password.

Now you can start the containers.

```bash
docker compose up -d
```

If you want to enter into the postgre docker container: `docker compose exec postgres bash`

If you want to enter into the postgre docker container: `docker compose exec airflow bash`

## Connect to the web server using a browser

`localhost:8080`

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

## Shutdown the dockers

```bash
docker compose down -v
```

## Test the cosipy DAG

1. Activate the DAG `"cosipy_contactsimulator"`.

    This is equivalent to the following steps:

    * Enter in the docker `airflow`

        ```bash
        docker compose exec airflow bash
        ```

    * Download the data file from wasabi.

        ```bash
        cd /shared_dir/pipeline
        source activate cosipy
        python initialize_pipeline.py
        ```

        This script downloads the input file from wasabi and move it in `/home/gamma/workspace/data`

2. Now we must activate the DAG named `"cosipt_test_v0"` from the airflow website

3. In case we didn't activated the first DAG `cosipy_contactsimulator`, then we have to copy the file in the input directory to trigger the DAG

    ```bash
    cd /home/gamma/workspace/data
    cp GalacticScan.inc1.id1.crab2hr.extracted.tra.gz input
    ```

4. Finally, 2e should see that the DAG started to process the data.

    This directory `/home/gamma/workspace/heasarc/dl0` contains several folders with this format `2025-01-24_14-31-56`.

    Inside the folder we have the results of the analysis.

We can visualize the results at the following link:

`localhost:8081`