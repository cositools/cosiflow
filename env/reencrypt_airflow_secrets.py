"""Re-save plaintext Airflow Connection/Variable values under the active Fernet key."""

from airflow.models import Connection, Variable
from airflow.utils.session import create_session


def main() -> None:
    connections_updated = 0
    variables_updated = 0
    with create_session() as session:
        for connection in session.query(Connection).all():
            if connection.password and not connection.is_encrypted:
                password = connection.password
                connection.password = password
                connections_updated += 1
            if connection.extra and not connection.is_extra_encrypted:
                extra = connection.extra
                connection.extra = extra
                connections_updated += 1
        for variable in session.query(Variable).all():
            if variable.val and not variable.is_encrypted:
                value = variable.val
                variable.val = value
                variables_updated += 1
    print(
        "Re-encrypted plaintext Airflow metadata: "
        f"connections={connections_updated}, variables={variables_updated}"
    )


if __name__ == "__main__":
    main()
