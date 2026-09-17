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
| `tests/test_issue18_contracts.py` | Review 18 | Removal of the unused paths module and the supported COSIDAG import contract |

`tests/security/support.py` provides repository-path and script-loading helpers;
it does not contain test cases.

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
