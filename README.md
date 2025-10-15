# Cosiflow

Cosiflow provides an Airflow-based orchestration environment for managing and monitoring scientific pipelines for COSI.

---

### 1. REQUIREMENTS

#### PREPARE THE ENVIRONMENT FILE

1. Copy and rename the `.env.example` file as `.env`:
   ```bash
   cp .env.example .env
   ```

2. Open the `.env` file and set a secure password for Airflow:
   ```bash
   AIRFLOW_ADMIN_PASSWORD=<YOUR_AIRFLOW_PASSWORD>
   ```

3. Manually bootstrap user and group IDs for the container:

   ```bash
   id -u
   # Copy the output as your USER_ID

   id -g
   # Copy the output as your GROUP_ID
   ```

4. Open `.env` and modify the following environment variables:

   ```bash
   UID=<YOUR_USER_ID>
   GID=<YOUR_GROUP_ID>
   ```

5. Open the `Dockerfile.airflow` file and paste the same values:

   ```dockerfile
   ARG MY_UID=<YOUR_USER_ID>
   ARG MY_GID=<YOUR_GROUP_ID>
   ```

---

### 2. BUILD THE COMPOSE

Build all containers defined in `docker-compose.yml`:

```bash
docker compose build
```

⏱ Estimated build time: **~490 seconds**

---

### 3. RUN THE CONTAINER

To run with logs visible:

```bash
docker compose up
```

To run in detached mode (no logs):

```bash
docker compose up -d
```

---

### 4. ENTER THE CONTAINER

To open a terminal inside the running Airflow container:

```bash
docker compose exec airflow bash
```

---

### 5. CONNECT TO THE AIRFLOW WEB UI

1. Open your web browser and go to:

   [http://localhost:8080/home](http://localhost:8080/home)

2. Insert the user credentials:
   ```text
   user:     admin
   password: <YOUR_AIRFLOW_PASSWORD>
   ```

---

### 6. STOP THE CONTAINER

To stop and remove all running containers, networks, and volumes:

```bash
docker compose down -v
```

---

### 7. CONFIGURATIONS

Below is the list of environment variables defined in `.env` with their purpose:

| Variable | Description |
|-----------|--------------|
| **UID** | User ID for container bootstrap |
| **GID** | Group ID for container bootstrap |
| **DISPLAY** | Display variable for X11 forwarding (optional) |
| **AIRFLOW_ADMIN_USERNAME** | Default Airflow Web UI username |
| **AIRFLOW_ADMIN_EMAIL** | Email associated with Airflow admin user |
| **AIRFLOW_ADMIN_PASSWORD** | Secure password for Airflow Web UI |
| **ALERT_USERS_LIST_PATH** | Path to YAML file containing user alert configurations |
| **ALERT_SMTP_SERVER** | SMTP server used for alert notifications |
| **ALERT_SMTP_PORT** | Port of the SMTP server |
| **ALERT_EMAIL_SENDER** | Email address used as sender for system alerts |
| **ALERT_LOG_PATH** | Path to Airflow log file monitored by alert system |
| **AIRFLOW__SMTP__SMTP_STARTTLS** | Enables/disables STARTTLS (default: False) |
| **AIRFLOW__SMTP__SMTP_SSL** | Enables/disables SMTP over SSL (default: False) |
| **MAILHOG_WEBUI_URL** | URL for MailHog web interface (for testing alerts) |
| **COSI_DATA_DIR** | Root directory for COSI data |
| **COSI_INPUT_DIR** | Directory for COSI input data |
| **COSI_LOG_DIR** | Directory for COSI log files |
| **COSI_OBS_DIR** | Directory for observation data |
| **COSI_TRANSIENT_DIR** | Directory for transient event data |
| **COSI_TRIGGER_DIR** | Directory for trigger event data |
| **COSI_MAPS_DIR** | Directory for map data products |
| **COSI_SOURCE_DIR** | Directory for source-level data products |

---

### NOTES

- Make sure `.env` and `docker-compose.yml` are located in the same directory.
- Do **not** commit your personal `.env` file to version control.
- To inspect container logs, use:
  ```bash
  docker compose logs -f airflow
  ```

---

**Cosiflow environment ready for use.**
