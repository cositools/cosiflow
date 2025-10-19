# COSIfest Mini‑Tutorial — Building Two DAGs in Airflow
Riccardo Falco — Cosiflow / COSI-AIRFLOW

---

## 0) What we’ll build
- **Exercise 1 — Hello World DAG**
  - `BashOperator` → create folder & `result.txt`
  - `PythonOperator` → append `"Hello Wolrd!"` to the file
- **Exercise 2 — Alice & Bob**
  - Two DAGs running “in parallel”, communicating via **filesystem**
  - **Alice (ExternalPythonOperator in `cosipy`)**: render 48×48 text → factorize via SVD → save `A`, `B` and plots
  - **Bob (ExternalPythonOperator + PythonSensor)**: wait for `factors.pkl` → reconstruct `A@B` → save plots

---

## 1) Airflow basics recap
- **DAG**: Directed Acyclic Graph — a workflow
- **Task**: a node in the DAG (atomic step)
- **Operators**:
  - `BashOperator`: run shell commands
  - `PythonOperator`: run a Python callable in the **Airflow** runtime
  - `ExternalPythonOperator`: run a Python callable in an **external interpreter** (Conda env)
- **Sensors**: tasks that **wait** for a condition (file exists, external task completes, etc.)

---

## 2) Exercise 1 — Folder & file (BashOperator)
**Goal**: create `/home/gamma/workspace/data/tutorials/result.txt`
```python
from airflow.operators.bash import BashOperator

make_file = BashOperator(
    task_id="make_folder_and_file",
    bash_command=(
        "mkdir -p /home/gamma/workspace/data/tutorials && "
        "touch /home/gamma/workspace/data/tutorials/result.txt"
    ),
)
```
**Tip**: use `mkdir -p` to be idempotent.

---

## 3) Exercise 1 — Append text (PythonOperator)
**Goal**: append `"Hello Wolrd!"` (typo kept) into the file.
```python
from airflow.operators.python import PythonOperator
from pathlib import Path

BASE = Path("/home/gamma/workspace/data/tutorials")
RESULT = BASE / "result.txt"

def write_hello():
    with open(RESULT, "a", encoding="utf-8") as f:
        f.write("Hello Wolrd!\n")

write_text = PythonOperator(
    task_id="write_text",
    python_callable=write_hello,
)
```
**Flow**: `make_file >> write_text`

---

## 4) Run & verify Exercise 1
- Trigger **hello_world_dag**
- Verify on the host/container:
```
cat /home/gamma/workspace/data/tutorials/result.txt
```
- You should see the line `Hello Wolrd!` appended.

---

## 5) Why ExternalPythonOperator for Exercise 2?
- Isolate scientific dependencies in **Conda env** (here: `cosipy`):
  - `EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy/bin/python"`
- Clean separation between orchestration (Airflow) and heavy libs
- Caveat: **no Airflow context** inside external process (fine for this demo)

---

## 6) Exercise 2 — Architecture
**Alice** (producer):
- Render multiline text into a tiny canvas → 0/1 matrix `X`
- SVD factorization: `X ≈ A @ B`
- Save: `factors.pkl` + plots `factor_A.png`, `factor_B.png`

**Bob** (consumer):
- `PythonSensor` waits for `factors.pkl`
- Load `A`, `B`; compute `M = A @ B`
- Save `reconstruction_float.png` and `reconstruction_binary.png`

Communication: **filesystem** at
`/home/gamma/workspace/data/tutorials/alice_bob_factor/`

---

## 7) Exercise 2 — Alice DAG (key pattern)
```python
from airflow.operators.python import ExternalPythonOperator

EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy/bin/python"

alice_factorize = ExternalPythonOperator(
    task_id="alice_factorize_text_matrix",
    python=EXTERNAL_PYTHON,
    python_callable=_alice_make_factors,   # defined in the DAG file
    op_kwargs={
        "base_dir": "/home/gamma/workspace/data/tutorials/alice_bob_factor",
        "text": "DAGs\n  ARE\nCOOL!",
        "size": [48, 48],
        "font_size": 6,
        "rank": 12,
    },
)
```
**Rule**: pass everything via **`op_kwargs`** to avoid global‑scope issues.

---

## 8) Exercise 2 — Bob DAG (sensor + external python)
```python
from airflow.sensors.python import PythonSensor
from airflow.operators.python import ExternalPythonOperator


wait_for_factors = PythonSensor(
    task_id="wait_for_factors_pickle",
    python_callable=_file_exists,
    op_kwargs={"pkl_path": "/.../factors.pkl"},
    poke_interval=10, timeout=3600,
)

bob_reconstruct = ExternalPythonOperator(
    task_id="bob_reconstruct_and_plot",
    python=EXTERNAL_PYTHON,
    python_callable=_bob_reconstruct_and_plot,
    op_kwargs={
        "base_dir": "/.../alice_bob_factor",
        "pkl_path": "/.../alice_bob_factor/factors.pkl",
        "bin_thr": 0.5,
    },
)

wait_for_factors >> bob_reconstruct
```

---

## 9) Demo flow
1. Trigger **Bob** first → observe the Sensor waiting
2. Trigger **Alice** → produces factors & plots
3. Bob continues → produces reconstructions
4. Show files in the shared folder

Cleanup (optional):
```
rm -f /home/gamma/workspace/data/tutorials/alice_bob_factor/*
```
