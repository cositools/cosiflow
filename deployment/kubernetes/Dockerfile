# Optional cluster image. Build the unchanged base image from env/ first.
ARG COSIFLOW_BASE_IMAGE=cosiflow-airflow:native
FROM ${COSIFLOW_BASE_IMAGE}
USER gamma
# Pin Airflow on subsequent installs so the provider cannot upgrade it.
RUN /home/gamma/venv/bin/pip install --no-cache-dir \
      'apache-airflow==2.10.3' 'apache-airflow-providers-cncf-kubernetes==9.0.1' \
    && /home/gamma/venv/bin/pip check
# Build context: the COSIflow repository root. Scientific source/venvs are
# installed separately on the shared Linux volume, using FasTP's own config.
COPY --chown=gamma modules/ /home/gamma/airflow/modules/
COPY --chown=gamma callbacks/ /home/gamma/airflow/callbacks/
COPY --chown=gamma plugins/ /home/gamma/airflow/plugins/
COPY --chown=gamma env/airflow.cfg /home/gamma/airflow/airflow.cfg
COPY --chown=gamma env/hot_load_module.sh /home/gamma/hot_load_module.sh
COPY --chown=gamma deployment/kubernetes/worker.sh /home/gamma/kubernetes-worker.sh
