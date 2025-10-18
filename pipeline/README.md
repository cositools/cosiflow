# Pipeline

## Requirements

1. Enter in the container `airflow`
    ```bash
    docker compose exec -it airflow /bin/bash
    ````

    Inside the container, if you didn't yet, initialize conda
    ```bash
    conda init
    ```
    then make `exit` to close the shell and re-enter in the container.
    ```bash
    exit
    docker compose exec -it airflow /bin/bash
    ````

    Then, activate the `cosipy` env
    ```bash
    conda activate cosipy
    ``` 


2. Only the first time you need to download the data to do that execute this command
    ```bash
    cd airflow/pipeline
    python download_data.py
    ```
    This script will download all the data required to execute the example pipelines.

    The data will be stored in the following path `/home/gamma/workspace/data/raw`

    Then to start the pipeline, you will need to activate the DAG in the web UI, and then execute the corresponding script which will simulate the data arrival.

### TSmap plot pipeline
To start the pipeline you need to execute the following script:
```bash
cd airflow/pipeline
python start_lcurvepipe.py
```
Then you can activate the DAG named `cosipipe_lightcurve`

### Light Curve plot pipeline
To start the pipeline you need to execute the following script:
```bash
cd airflow/pipeline
python start_tsmappipe.py
```
Then you can activate the DAG named `cosipipe_tsmap`
