# COSIflow

COSIflow provides the Apache Airflow runtime and reusable components used to
orchestrate COSI scientific pipelines. Scientific workflows remain in their
own module repositories; COSIflow supplies the scheduler, module loader,
`COSIDAG` framework, shared callbacks, operator-facing plugins, and a durable
GCN inbox and outbox.

## Start here

- [Install the local development stack](getting-started/installation.md).
- [Configure secrets, ports, data paths, and startup](getting-started/configuration.md).
- [Install or update a scientific module](architecture/modules.md).
- [Build a filesystem-driven workflow with COSIDAG](reference/cosidag.md).
- [Operate the GCN client](reference/gcn-client.md).
- [Understand plugin routes and permissions](plugins/index.md).

## Repository responsibilities

| Area | COSIflow responsibility |
| --- | --- |
| Orchestration | Airflow webserver, scheduler, metadata database, and initialization |
| Framework | COSIDAG monitoring, input resolution, state, retriggering, and result link contract |
| Extensibility | Module discovery, links, managed Python environments, and optional image builds |
| Operator UI | Data, GCN notice, DAG refresh, COSIDAG state, and MailHog views |
| Alert transport | GCN Kafka consumer/producer workers backed by a dedicated MySQL database |

FasTP owns its scientific DAGs, parameters, runtimes, and products. Its
[repository documentation](https://github.com/cositools/fast-transient-analysis-pipeline)
describes those contracts and how the module integrates with COSIflow.

## Current scope

The checked-in Compose stack is a local-development deployment. Its UI ports
are loopback-only, its GCN producer is disabled by default, and real notice
publication requires separate authorization and operational review. This
documentation does not claim that the local stack is a production deployment.
