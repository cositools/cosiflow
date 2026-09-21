# Regression testing

COSIflow keeps repository-level regression tests under `tests/`. The suites
protect supported behavior owned by the Review issues and are intentionally
separate from production acceptance tests.

Test selection and CI integration are tracked in
[Review 14](https://github.com/cositools/cosiflow/issues/15). A test is retained
only after the repository owner has reviewed the behavior, assertions,
fixtures, dependencies, runtime, and maintenance cost.

## Run the tests

From the COSIflow repository root, run the complete committed suite with:

```bash
python3 -m unittest discover -s tests -v
```

Run only the security regression tests with:

```bash
python3 -m unittest discover -s tests/security -t tests -v
```

The security suite uses only the Python standard library. It does not require
Airflow, Docker containers, database servers, real credentials, or network
access. Tests that require an optional runtime dependency must declare and
report an explicit skip when that dependency is unavailable.

## Suite layout

| Path | Owning Review | Protected behavior |
| --- | --- | --- |
| `tests/security/test_validate_runtime_secrets.py` | Review 5 | Required values, minimum lengths, revoked-value detection, Airflow key separation, Fernet format, PostgreSQL connection validation, DSN escaping, and prevention of secret disclosure in errors |
| `tests/security/test_bootstrap_secrets.py` | Review 5 | Secret generation and rotation, preservation of unmanaged configuration, replacement of revoked values, generated-key validity, and `.env` permissions |
| `tests/security/test_compose_security.py` | Review 5 | Required Compose secrets, Docker-daemon isolation, loopback-only published ports, and internal database networking |
| `tests/security/test_rotation_guards.py` | Review 5 | Backup confirmation, presence of old credentials, safe generated-password format, and refusal before Docker or database access when preconditions fail |
| `tests/security/test_shared_auth.py` | Review 6 | Anonymous redirect, fail-closed authorization, absence of pre-authorization side effects, capability checks, and the Viewer/Scientist/Operator/Admin permission matrix |
| `tests/security/test_endpoint_matrix.py` | Review 6 | Complete route-to-permission coverage, POST-only mutations, least-privilege manifests, and CSRF tokens in mutating forms and requests |
| `tests/security/test_configure_rbac.py` | Review 6 | Viewer isolation, Scientist/Operator/Admin provisioning, stale-permission removal, verification failures, and idempotent reconciliation |
| `tests/test_issue18_contracts.py` | Review 18 | Removal of the unused paths module and the supported COSIDAG import contract |
| `tests/test_review8_cosidag_state.py` | Review 8 | Runtime retrigger identity/configuration, transactional schema, claim/finalization wiring, migration, and reset-plugin contracts |
| `tests/integration/test_review8_postgres.py` | Review 8 | Real PostgreSQL uniqueness, concurrent claims, failure recovery, idempotent finalization, and reset isolation |
| `tests/test_review12_worker_resilience.py` | Review 12 | Worker backoff and failure budgets, poison-row isolation, supervisor fallback, stale heartbeat handling, and Compose restart/healthcheck contracts |
| `tests/integration/test_review12_mysql.py` | Review 12 | Real MySQL retry scheduling, stale-lock recovery, stale healthcheck failure, and container restart behavior |
| `tests/test_review13_module_loader_safety.py` | Review 13 | YAML schema validation, destructive-path confinement, overlapping-environment rejection, traversal and symlink rejection, shell-payload isolation, and mutation-free install/update/remove failures |

`tests/security/support.py` and `tests/security/auth_support.py` provide
repository-path, script-loading, and Auth Manager test doubles; they do not
contain test cases.

## Review 8 transactional-state tests

The Review 8 unit/contract suite is part of the ordinary command above and has
no optional dependencies. Run it alone with:

```bash
python3 -m unittest tests.test_review8_cosidag_state -v
```

The PostgreSQL integration suite uses a disposable Compose project backed by a
`tmpfs`. It applies the schema twice to verify idempotency and removes the
container and volume after the run:

```bash
COSIFLOW_RUN_REVIEW8_POSTGRES_TESTS=1 \
  python3 -m unittest tests.integration.test_review8_postgres -v
```

Docker and Compose are required only for this focused integration suite. It
does not connect to the development or production COSIflow databases.

## Review 12 worker-resilience tests

The Review 12 unit suite is part of the ordinary command above. Its injected
failures use test doubles and replace real sleeps with recorded delays:

```bash
python3 -m unittest tests.test_review12_worker_resilience -v
```

The optional integration suite uses disposable MySQL storage and a dedicated
Compose project. It builds the GCN client image, verifies the real retry and
lock-recovery SQL, checks a stale application heartbeat, and confirms that an
intentionally failing probe container is restarted by `unless-stopped`:

```bash
COSIFLOW_RUN_REVIEW12_MYSQL_TESTS=1 \
  python3 -m unittest tests.integration.test_review12_mysql -v
```

The suite removes its containers and volumes after the run. It uses synthetic
test-only credentials and does not connect to GCN or the development database.

## Review 13 module-loader safety tests

The Review 13 suite requires PyYAML and otherwise uses temporary directories
and a fake `docker` executable. It does not start containers, create virtual
environments, remove real paths, or build images:

```bash
python3 -m unittest tests.test_review13_module_loader_safety -v
```

The parser tests cover valid quoted YAML, duplicate keys, malformed documents,
wrong types, unsafe identifiers, destructive paths, and duplicate or nested
environment targets. The shell tests verify that invalid input produces no
mutating Docker call, that removal rejects a missing module and preflights its
configured paths, and that valid arguments, including supported paths with
spaces, remain single argv values.

## Review 5 safety model

The Review 5 suite tests failure paths without exposing or changing operational
state:

- environment dictionaries contain synthetic values only;
- `.env` files are created inside temporary directories and deleted after each
  test;
- secret values are never written to test output;
- rotation guard tests stop before any database operation;
- a fake `docker` executable verifies that invalid input cannot reach Docker;
- Compose isolation checks inspect the committed runtime manifests without
  starting services.

The suite does not perform a real PostgreSQL or MySQL credential rotation. A
real rotation changes persistent database state and requires verified backups,
controlled credentials, and explicit operational approval. That evidence is a
deployment or maintenance activity rather than a routine unit-test fixture.

## Review 6 authorization model

The Review 6 suite verifies both policy declarations and execution order:

- every exposed AppBuilder route must match the canonical endpoint manifest;
- anonymous requests redirect to login before the protected view runs;
- missing permissions, missing users, and Auth Manager errors return 403 before
  protected code runs;
- Viewer, Scientist, Operator, and Admin receive the expected capabilities;
- RBAC reconciliation grants Admin every COSIflow permission explicitly while
  route authorization remains permission-based rather than role-name-based;
- repeated RBAC reconciliation is idempotent;
- mutating routes are POST-only and their templates submit CSRF tokens.

The tests use deterministic Auth Manager and FAB security-manager doubles so
they run without Airflow. They exercise the same public method contract used by
the plugins, but do not replace an authenticated smoke test in the supported
Airflow image or production SSO validation.

## Adding or changing a regression test

For every proposed test:

1. identify the owning Review issue and the supported behavior being protected;
2. prefer deterministic local fixtures and synthetic values;
3. avoid real credentials, sensitive payloads, external network calls, and
   production assumptions;
4. prove denied or invalid operations have no side effects where relevant;
5. document optional dependencies and justified skips;
6. run the focused suite and the complete suite;
7. obtain repository-owner approval before merge.

Do not retain a generated test merely because it passed once. Tests that lock
down incidental implementation details, depend on unstable external services,
or impose unjustified maintenance cost should be revised or explicitly
rejected in Review 14.

## CI and production boundary

Review 14 owns the pull-request CI workflow for approved unit, integration,
characterization, and regression tests. Until that workflow is merged, the
commands above are the authoritative local verification commands.

Production-only checks remain outside this suite. They include SSO mapping,
production secret delivery, TLS and ingress, monitoring, backup and restore,
failover, rollback, and representative end-to-end workflow execution on the
target environment.
