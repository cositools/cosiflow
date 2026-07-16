FROM alpine/git AS gcn-schema

ARG GCN_SCHEMA_REF=478395f5a5ff575b0519b0c54491f3365d1d0aac

WORKDIR /schemas

RUN git init \
    && git remote add origin https://github.com/cositools/gcn-schema.git \
    && git sparse-checkout init --cone \
    && git sparse-checkout set gcn/notices/cosi gcn/notices/core \
    && git fetch --depth=1 origin "${GCN_SCHEMA_REF}" \
    && git checkout FETCH_HEAD


FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY examples /app/examples

COPY --from=gcn-schema \
    /schemas/gcn/notices/cosi \
    /app/gcn-schema/gcn/notices/cosi

COPY --from=gcn-schema \
    /schemas/gcn/notices/core \
    /app/gcn-schema/gcn/notices/core

CMD ["python", "-m", "app.main", "run"]