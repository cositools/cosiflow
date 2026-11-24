#!/bin/bash
set -euo pipefail

[ "${1:-}" = "--backup-confirmed" ] || {
    printf 'Refusing rotation: create and verify database backups, then pass --backup-confirmed.\n' >&2
    exit 2
}

for name in OLD_POSTGRES_PASSWORD OLD_GCN_DB_PASSWORD OLD_GCN_MYSQL_ROOT_PASSWORD; do
    [ -n "${!name:-}" ] || {
        printf '%s is required for negative post-rotation verification.\n' "$name" >&2
        exit 2
    }
done

env_value() {
    awk -v key="$1" 'index($0, key "=") == 1 { print substr($0, length(key) + 2); exit }' .env
}

new_postgres_password="$(env_value POSTGRES_PASSWORD)"
new_gcn_password="$(env_value GCN_DB_PASSWORD)"
new_gcn_root_password="$(env_value GCN_MYSQL_ROOT_PASSWORD)"
postgres_user="$(env_value POSTGRES_USER)"
postgres_db="$(env_value POSTGRES_DB)"
gcn_user="$(env_value GCN_DB_USER)"
gcn_db="$(env_value GCN_DB_NAME)"
postgres_user="${postgres_user:-airflow_user}"
postgres_db="${postgres_db:-airflow_db}"
gcn_user="${gcn_user:-gcn_user}"
gcn_db="${gcn_db:-gcn}"

for value in "$postgres_user" "$postgres_db" "$gcn_user" "$gcn_db"; do
    [[ "$value" =~ ^[A-Za-z0-9_]+$ ]] || {
        printf 'Database identifiers must contain only letters, digits, and underscore.\n' >&2
        exit 2
    }
done
for value in "$new_postgres_password" "$new_gcn_password" "$new_gcn_root_password"; do
    [[ "$value" =~ ^[A-Za-z0-9_-]{24,}$ ]] || {
        printf 'Generated database passwords are missing or have an unsafe format.\n' >&2
        exit 2
    }
done

workspace="$(cd .. && pwd)"
postgres_container="cosiflow-issue5-postgres-rotation"
mysql_container="cosiflow-issue5-mysql-rotation"

cleanup() {
    docker stop "$postgres_container" "$mysql_container" >/dev/null 2>&1 || true
    docker rm "$postgres_container" "$mysql_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --name "$postgres_container" -d \
    -v "$workspace/data/postgres_data:/var/lib/postgresql/data" postgres:15 >/dev/null
docker run --name "$mysql_container" -d \
    -e MYSQL_ROOT_PASSWORD="$OLD_GCN_MYSQL_ROOT_PASSWORD" \
    -e TARGET_ROOT_PASSWORD="$new_gcn_root_password" \
    -v "$workspace/data/gcn_mysql_data:/var/lib/mysql" mysql:8.4 >/dev/null

for _ in $(seq 1 60); do
    docker exec "$postgres_container" pg_isready -U "$postgres_user" -d "$postgres_db" >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$postgres_container" pg_isready -U "$postgres_user" -d "$postgres_db" >/dev/null

for _ in $(seq 1 60); do
    docker exec "$mysql_container" sh -c '
        MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysqladmin ping -uroot --silent 2>/dev/null ||
        MYSQL_PWD="$TARGET_ROOT_PASSWORD" mysqladmin ping -uroot --silent 2>/dev/null
    ' >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$mysql_container" sh -c '
    MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysqladmin ping -uroot --silent 2>/dev/null ||
    MYSQL_PWD="$TARGET_ROOT_PASSWORD" mysqladmin ping -uroot --silent 2>/dev/null
' >/dev/null

docker exec -i "$postgres_container" psql -v ON_ERROR_STOP=1 -U "$postgres_user" -d "$postgres_db" >/dev/null <<SQL
ALTER ROLE "$postgres_user" WITH PASSWORD '$new_postgres_password';
SQL

docker exec -i "$mysql_container" sh -c '
    if MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -e "SELECT 1" >/dev/null 2>&1; then
        export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"
    else
        export MYSQL_PWD="$TARGET_ROOT_PASSWORD"
    fi
    exec mysql -uroot
' >/dev/null <<SQL
ALTER USER '$gcn_user'@'%' IDENTIFIED BY '$new_gcn_password';
ALTER USER 'root'@'localhost' IDENTIFIED BY '$new_gcn_root_password';
ALTER USER IF EXISTS 'root'@'%' IDENTIFIED BY '$new_gcn_root_password';
FLUSH PRIVILEGES;
SQL

docker exec -i "$postgres_container" sh -c \
    'read -r password; host="$(hostname -i | awk "{print \$1}")"; PGPASSWORD="$password" psql -h "$host" -U "$1" -d "$2" -c "SELECT 1" >/dev/null' \
    sh "$postgres_user" "$postgres_db" <<<"$new_postgres_password"

if docker exec -i "$postgres_container" sh -c \
    'read -r password; host="$(hostname -i | awk "{print \$1}")"; PGPASSWORD="$password" psql -h "$host" -U "$1" -d "$2" -c "SELECT 1" >/dev/null 2>&1' \
    sh "$postgres_user" "$postgres_db" <<<"$OLD_POSTGRES_PASSWORD"; then
    printf 'Old PostgreSQL credential is still accepted.\n' >&2
    exit 1
fi

docker exec -i "$mysql_container" sh -c \
    'read -r password; MYSQL_PWD="$password" mysql -h 127.0.0.1 -u"$1" "$2" -e "SELECT 1" >/dev/null' \
    sh "$gcn_user" "$gcn_db" <<<"$new_gcn_password"

if docker exec -i "$mysql_container" sh -c \
    'read -r password; MYSQL_PWD="$password" mysql -h 127.0.0.1 -u"$1" "$2" -e "SELECT 1" >/dev/null 2>&1' \
    sh "$gcn_user" "$gcn_db" <<<"$OLD_GCN_DB_PASSWORD"; then
    printf 'Old GCN MySQL application credential is still accepted.\n' >&2
    exit 1
fi

docker exec -i "$mysql_container" sh -c \
    'read -r password; MYSQL_PWD="$password" mysql -h 127.0.0.1 -uroot -e "SELECT 1" >/dev/null' \
    <<<"$new_gcn_root_password"

if docker exec -i "$mysql_container" sh -c \
    'read -r password; MYSQL_PWD="$password" mysql -h 127.0.0.1 -uroot -e "SELECT 1" >/dev/null 2>&1' \
    <<<"$OLD_GCN_MYSQL_ROOT_PASSWORD"; then
    printf 'Old GCN MySQL root credential is still accepted.\n' >&2
    exit 1
fi

printf 'Database credentials rotated; all three previous credentials were rejected.\n'
