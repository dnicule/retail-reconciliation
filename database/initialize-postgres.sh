#!/bin/sh
# Initialize a fresh application schema through psql in the existing PostgreSQL container.
# Reuses the unchanged versioned migration; no host Node/npm or application data writes are needed.
set -eu

if [ "$#" -ne 0 ]; then
    printf '%s\n' 'Usage: sh database/initialize-postgres.sh' >&2
    exit 2
fi
if [ "$(uname -s)" != Darwin ]; then
    printf '%s\n' 'Run this preparation step from Terminal on a Mac.' >&2
    exit 1
fi

database_root=$(CDPATH= cd -P -- "$(dirname -- "$0")/.." && pwd -P)
database_migration="$database_root/dbInit/migrations/001_initial_reconciliation_schema.sql"
# Read the whole file before contacting Docker, and keep its original bytes for the receipt hash.
database_sql=$(cat "$database_migration")
database_hash_line=$(shasum -a 256 "$database_migration")
database_checksum=${database_hash_line%% *}
case "$database_checksum" in
    ''|*[!0-9a-f]*) printf '%s\n' 'Cannot calculate the migration checksum.' >&2; exit 1 ;;
esac
[ "${#database_checksum}" -eq 64 ] || exit 1

printf '%s\n' '[database] Preparing a fresh schema in postgres-postgres-1 using its POSTGRES_DB.'
# The migration uses CREATE without replacement. Existing objects or any error abort the transaction.
# psql commits the schema and its receipt together; shell interpolation does not re-evaluate SQL text.
if ! docker exec -i postgres-postgres-1 sh -c '
    export PGCONNECT_TIMEOUT=5
    exec psql --no-psqlrc --no-password --set ON_ERROR_STOP=on \
        --single-transaction --file=- --username="$POSTGRES_USER" --dbname="$POSTGRES_DB"
' <<SQL
SET LOCAL search_path = pg_catalog;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SELECT pg_advisory_xact_lock(1735293268, 1);
$database_sql
INSERT INTO public.schema_migrations(version, name, checksum)
VALUES (1, 'initial_reconciliation_schema', '$database_checksum');
SQL
then
    printf '%s\n' 'PostgreSQL initialization failed or its outcome is unconfirmed. Inspect the error before retrying; existing schemas must not be replaced.' >&2
    exit 1
fi

printf '%s\n' '[database] PostgreSQL schema and migration receipt committed. No products or jobs were inserted.'
