# Pipeline

## Overview

All example pipelines (Light Curve, TSMap, etc.) are initialized and triggered **through the Airflow Web UI** using the **`init_pipelines` DAG**.

Each scientific pipeline is implemented as a dedicated **COSIDAG** (e.g. Light Curve, TSMap), while `init_pipelines` acts as a **single entry point** responsible for initialization and routing.

The `init_pipelines` DAG is responsible for:

* staging all required inputs (response, orientation, source, background)
* validating paths and configuration
* selecting which scientific pipeline to run (`lcurve`, `tsmap`, …)
* defining **where outputs are stored** (Light Curve or TSMap folders)
* triggering the appropriate downstream COSIDAG automatically

**No manual scripts are required anymore to start individual pipelines.**

---

## Starting a pipeline (NEW workflow)

### Step 1 — Enable the scientific COSIDAG

Before triggering any pipeline, make sure that the **target COSIDAG** is enabled in the Airflow UI:

* enable `cosipipe_lightcurve` for Light Curve products
* enable `cosipipe_tsmap` for TS Map products

This is required only once (or after a DAG refresh).

---

### Step 2 — Use the `init_pipelines` DAG

To start **any pipeline**, follow these steps:

1. Open the **Airflow Web UI**

2. Enable the DAG named **`init_pipelines`**

3. Click **Trigger DAG**

4. Fill in the required parameters:

   * `response_path`
   * `orientation_path`
   * `source_path`
   * `background_path`
   * `destination` → choose where outputs will be saved:

     * `lcurve` → results stored in the Light Curve pipeline folder
     * `tsmap` → results stored in the TS Map pipeline folder
   * other optional parameters (e.g. time windows)

5. Click **Trigger**

That’s it.
The selected COSIDAG will be instantiated and executed automatically, with outputs routed to the chosen destination.

---

## Supported pipelines

| Destination value | Pipeline started     | Output location    |
| ----------------- | -------------------- | ------------------ |
| `lcurve`          | Light Curve plotting | Light Curve folder |
| `tsmap`           | TS Map generation    | TS Map folder      |

More destinations can be added without changing the user workflow.

---

## What changed (important)

### Old workflow (deprecated)

* Manual scripts such as:

  * `start_lcurvepipe.sh`
  * `start_tsmappipe.sh`
* Manual triggering of individual pipeline DAGs

### New workflow (current)

* **Single entry point:** `init_pipelines`
* Explicit activation of the target COSIDAG
* Fully UI-driven configuration
* Output destination selected at trigger time
* Cleaner, reproducible, and closer to production usage