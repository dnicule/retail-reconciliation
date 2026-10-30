#!/bin/sh
# Configure and start the independent local Docker services on macOS without replacing existing resources.
# Requires a local Linux Docker engine, Compose 2.30+, and curl; performs no application data writes.
set -eu
set -f

# Print the supported modes without inspecting files or contacting Docker.
# @returns Always succeeds; writes usage to stdout.
usage() {
    cat <<'USAGE'
Usage: sh ./setup.sh [--infrastructure] [--check]
       sh ./setup.sh --help

  --infrastructure  Select PostgreSQL, MongoDB, Grafana, Loki, and Alloy only.
  --check           Inspect selected services without changing files or containers.
  --help            Show this help without contacting Docker.

The default also selects the Update API and reconciliation worker.
Existing configuration, containers, volumes, and images are reused.
Missing private configuration is copied from templates; edit it before rerunning.
Application databases must already have compatible structures for full readiness.
No host Node.js, npm, or nvm is required.

Supported host: macOS only, with Docker Desktop or Rancher Desktop using Moby.
Exit codes: 0 success, 1 incomplete setup/check, 2 invalid arguments.
USAGE
}

# Report a non-secret operational error and terminate the current shell.
# @param $* Human-readable diagnostic. @returns Does not return (exit 1).
fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

# Emit a confirmed stage or operation outcome.
# @param $* Non-secret message. @returns Always succeeds.
note() {
    printf '[setup] %s\n' "$*"
}

setup_infrastructure=0
setup_check=0
for setup_argument in "$@"; do
    case "$setup_argument" in
        --help)
            if [ "$#" -ne 1 ]; then
                printf '%s\n' '--help must be used alone.' >&2
                exit 2
            fi
            usage
            exit 0
            ;;
        --infrastructure)
            [ "$setup_infrastructure" -eq 0 ] || { usage >&2; exit 2; }
            setup_infrastructure=1
            ;;
        --check)
            [ "$setup_check" -eq 0 ] || { usage >&2; exit 2; }
            setup_check=1
            ;;
        *) usage >&2; exit 2 ;;
    esac
done

# Check the host before accessing project files or Docker; --help remains informational.
setup_host=$(uname -s) || fail 'Cannot determine the host operating system.'
[ "$setup_host" = Darwin ] || fail 'setup.sh supports macOS only. Run it from Terminal on a Mac.'

setup_root=$(CDPATH= cd -P -- "$(dirname -- "$0")" && pwd -P) || exit 1
setup_deadline=$(($(date +%s) + 1800))
setup_locked=0
setup_lock=

# Execute one command with a time limit bounded by the overall 30-minute deadline.
# @param $1 Maximum seconds. @param $2... Command and literal arguments.
# @returns Command status; nonzero after interruption/timeout. Effects may be uncertain on timeout.
bounded() (
    bounded_seconds=$1
    shift
    bounded_remaining=$((setup_deadline - $(date +%s)))
    [ "$bounded_remaining" -gt 0 ] || fail 'Overall setup deadline exceeded; inspect current state before retrying.'
    if [ "$bounded_seconds" -gt "$bounded_remaining" ]; then
        bounded_seconds=$bounded_remaining
    fi
    bounded_started=$(date +%s)
    # Explicit stdin inheritance also supports piped readiness JSON/log validation.
    "$@" <&0 &
    bounded_pid=$!
    (
        bounded_elapsed=0
        while [ "$bounded_elapsed" -lt "$bounded_seconds" ]; do
            sleep 1
            bounded_elapsed=$((bounded_elapsed + 1))
        done
        printf '%s\n' 'Command timed out; its effects may be uncertain. Inspect state before retrying.' >&2
        kill -TERM "$bounded_pid" 2>/dev/null || exit 0
        sleep 2
        kill -KILL "$bounded_pid" 2>/dev/null || :
    ) </dev/null >&2 &
    bounded_watch=$!
    trap 'kill -TERM "$bounded_pid" "$bounded_watch" 2>/dev/null || :; exit 130' INT
    trap 'kill -TERM "$bounded_pid" "$bounded_watch" 2>/dev/null || :; exit 143' TERM HUP
    bounded_status=0
    wait "$bounded_pid" || bounded_status=$?
    kill -TERM "$bounded_watch" 2>/dev/null || :
    wait "$bounded_watch" 2>/dev/null || :
    trap - INT TERM HUP
    # A command can handle TERM and return zero; expiry must still report uncertainty, never success.
    if [ "$(($(date +%s) - bounded_started))" -ge "$bounded_seconds" ]; then
        exit 124
    fi
    exit "$bounded_status"
)

# Call Docker with a 30-second control-plane deadline.
# @param $* Docker arguments. @returns Docker status; stdout is preserved.
docker_call() {
    bounded 30 docker "$@"
}

# Invoke one explicit Compose project with deterministic paths and environment-file selection.
# @param $1 Timeout seconds. @param $2 Project name. @param $3... Compose arguments.
# @returns Compose status; never combines projects or sources shell environment files.
compose() (
    compose_seconds=$1
    compose_project=$2
    shift 2
    compose_env=/dev/null
    case "$compose_project" in
        postgres) compose_dir=postGres; compose_file=postGresCompose.yaml ;;
        mongodb) compose_dir=mongoDb; compose_file=mongoCompose.yaml ;;
        docker-observability)
            compose_dir=observability; compose_file=compose.yaml
            compose_env="$setup_root/observability/.env"
            ;;
        update-api) compose_dir=update-api; compose_file=compose.yaml ;;
        reconciliation-worker) compose_dir=reconciliation-worker; compose_file=compose.yaml ;;
        *) fail 'Unknown internal Compose project.' ;;
    esac
    # These are the established project names, not new names derived from the root folder.
    bounded "$compose_seconds" docker compose --project-name "$compose_project" \
        --project-directory "$setup_root/$compose_dir" --env-file "$compose_env" \
        -f "$setup_root/$compose_dir/$compose_file" "$@"
)

# List project/service pairs in dependency order; backend pairs are excluded in infrastructure mode.
# @returns A whitespace-separated, fixed inventory; performs no external operations.
services() {
    printf '%s\n' 'postgres postgres' 'mongodb mongo' 'docker-observability loki' \
        'docker-observability alloy' 'docker-observability grafana'
    if [ "$setup_infrastructure" -eq 0 ]; then
        printf '%s\n' 'update-api update-api' 'reconciliation-worker reconciliation-worker'
    fi
}

# Find exactly one Compose-owned service container, including stopped containers.
# @param $1 Project. @param $2 Service. @returns ID or empty stdout; fails on ambiguity/read failure.
container_id() (
    container_ids=$(docker_call ps -aq --filter "label=com.docker.compose.project=$1" \
        --filter "label=com.docker.compose.service=$2" \
        --filter 'label=com.docker.compose.oneoff=False') || fail "Cannot inspect container ownership for $2."
    set -- $container_ids
    [ "$#" -le 1 ] || fail 'Multiple containers match one selected service; no changes made to them.'
    printf '%s\n' "${1-}"
)

# Check private files; copy only absent templates in mutating modes using atomic no-clobber creation.
# @param $1 Relative filename. @returns 0 if already present, 1 if missing/created; fails on I/O errors.
configuration_file() (
    configuration_path="$setup_root/$1"
    if [ -f "$configuration_path" ]; then
        [ -r "$configuration_path" ] || fail "Cannot read $1."
        return 0
    fi
    [ ! -e "$configuration_path" ] && [ ! -L "$configuration_path" ] || fail "Unexpected file type at $1."
    if [ "$setup_check" -eq 1 ]; then
        note "Missing configuration: $1 (check mode leaves it absent)."
    else
        [ -f "$configuration_path.example" ] || fail "Missing template: $1.example."
        (umask 077; set -C; cat "$configuration_path.example" > "$configuration_path") || \
            fail "Cannot create $1 without overwriting another file."
        note "Created $1; edit its required settings before rerunning."
    fi
    return 1
)
resolved_config_hash() (
    hash_project=$1
    hash_service=$2
    #Older compose versions omit env_file values in config
    hash_model=$(compose 30 "$hash_project" config --format json "$hash_service" 2>/dev/null) || fail "Cannot resolve configuration for $hash_service."
    [ -n "$hash_model" ] || fail "Empty resolved configuration for $hash_service."
    #Copose's renderer escapes literal dollar signs as $$. Keep interpolation enabled on this
    #second pass to restore the literal values;
    hash_line=$(bounded 30 docker compose --project-name "$hash_project" \
        --project-directory "$setup_root" --env-file /dev/null -f - \
        config --hash "$hash_service" 2>/dev/null <<EOF
$hash_model
EOF
    ) || fail "Cannot calculate resolved configuration identiy for $hash_service."
    set -- $hash_line
    [ "$#" -eq 2 ] && [ "$1" = "$hash_service" ] || fail "Unexpected configuration hash output for $hash_service."
    case "$2" in
        ''|*[!0-9a-f]*) fail "Invalid configuration hash for $hash_service." ;;
    esac
    [ "${#2}" -eq 64 ] || fail "Invalid configuration has length for $hash_service."
    printf "%s\n" "$2" 
)
# Validate an existing service's configuration, network and persistent-volume ownership.
# @param $1 Project. @param $2 Service. @param $3 Container ID. @returns 0 or exits on drift/uncertainty.
validate_container() (
    validate_project=$1
    validate_service=$2
    validate_id=$3
    validate_hash=$(resolved_config_hash "$validate_project" "$validate_service") || exit 1
    validate_actual=$(docker_call inspect --format '{{index .Config.Labels "com.docker.compose.config-hash"}}' "$validate_id") || \
        fail "Cannot inspect $validate_service configuration identity."
    [ -n "$validate_hash" ] && [ "$validate_hash" = "$validate_actual" ] || \
        fail "$validate_service resolved configuration differs from container $validate_id in Docker context $setup_context. Check .env.docker, Compose settings, and the Composer version used to create it. Automatic recreation is disabled."
    validate_network=$(docker_call inspect --format '{{if index .NetworkSettings.Networks "glue-local"}}attached{{end}}' "$validate_id") || \
        fail "Cannot inspect $validate_service network."
    [ "$validate_network" = attached ] || fail "$validate_service is not attached to glue-local."
    validate_ports=$(docker_call inspect --format '{{range .HostConfig.PortBindings}}{{range .}}{{if eq .HostIp "127.0.0.1"}}loopback{{else}}unsafe{{end}}{{println}}{{end}}{{end}}' "$validate_id") || \
        fail "Cannot inspect $validate_service port bindings."
    for validate_ip in $validate_ports; do
        [ "$validate_ip" = loopback ] || fail "$validate_service has a non-loopback port binding."
    done
    case "$validate_service" in
        postgres) validate_mount='postgres_postgres-data /var/lib/postgresql' ;;
        mongo) validate_mount='mongodb_mongo-data /data/db' ;;
        loki) validate_mount='docker-observability_loki-data /loki' ;;
        alloy) validate_mount='docker-observability_alloy-data /var/lib/alloy/data' ;;
        grafana) validate_mount='docker-observability_grafana-data /var/lib/grafana' ;;
        *) validate_mount= ;;
    esac
    if [ -n "$validate_mount" ]; then
        validate_mounts=$(docker_call inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{println .Name .Destination}}{{end}}{{end}}' "$validate_id") || \
            fail "Cannot inspect $validate_service volume ownership."
        printf '%s\n' "$validate_mounts" | grep -Fx "$validate_mount" >/dev/null || \
            fail "$validate_service uses an unexpected persistent volume."
    fi
)

# Refuse adoption of colliding container names or volumes owned by another project.
# @param $1 Project. @param $2 Service. @returns 0 for absent/owned resources; exits on failed reads or conflicts.
validate_missing_service() (
    missing_project=$1
    missing_service=$2
    case "$missing_service" in
        grafana|alloy|loki) missing_name="observability-$missing_service-1" ;;
        *) missing_name="$missing_project-$missing_service-1" ;;
    esac
    missing_names=$(docker_call ps -a --format '{{.Names}}') || fail 'Cannot inspect existing container names.'
    if printf '%s\n' "$missing_names" | grep -Fx "$missing_name" >/dev/null; then
        fail "Container name $missing_name is already used without the expected ownership labels."
    fi
    case "$missing_service" in
        postgres) missing_volume=postgres-data ;;
        mongo) missing_volume=mongo-data ;;
        grafana|alloy|loki) missing_volume="$missing_service-data" ;;
        *) return 0 ;;
    esac
    missing_volume_name="${missing_project}_$missing_volume"
    missing_volumes=$(docker_call volume ls --format '{{.Name}}') || fail 'Cannot inventory Docker volumes.'
    if printf '%s\n' "$missing_volumes" | grep -Fx "$missing_volume_name" >/dev/null; then
        missing_owner=$(docker_call volume inspect --format '{{index .Labels "com.docker.compose.project"}} {{index .Labels "com.docker.compose.volume"}}' "$missing_volume_name") || \
            fail "Cannot inspect ownership of $missing_volume_name."
        [ "$missing_owner" = "$missing_project $missing_volume" ] || fail "Volume $missing_volume_name has unconfirmed ownership; it will not be adopted."
    fi
)

# Reuse a matching installed image or obtain only the missing selected service image.
# @param $1 Project. @param $2 Service. @returns 0 when image is available; fails without treating reads as absence.
ensure_image() (
    image_project=$1
    image_service=$2
    image_name=$(compose 30 "$image_project" config --images "$image_service" 2>/dev/null) || \
        fail "Cannot resolve the image for $image_service."
    [ -n "$image_name" ] || fail "No configured image for $image_service."
    image_ids=$(docker_call image ls --quiet "$image_name") || fail "Cannot inventory images for $image_service."
    if [ -n "$image_ids" ]; then
        note "Reusing installed image for $image_service."
        return 0
    fi
    case "$image_service" in
        update-api|reconciliation-worker)
            note "Building missing image for $image_service."
            compose 600 "$image_project" build "$image_service" || fail "Image build failed for $image_service."
            ;;
        *)
            note "Pulling missing image for $image_service."
            compose 600 "$image_project" pull "$image_service" || fail "Image pull failed for $image_service."
            ;;
    esac
)

# Start/create only one selected service, retaining all already-existing containers and volumes.
# @param $1 Project. @param $2 Service. @returns 0 on confirmed running/create start; failures retain state.
ensure_service() (
    service_project=$1
    service_name=$2
    service_id=$(container_id "$service_project" "$service_name") || exit 1
    if [ -n "$service_id" ]; then
        validate_container "$service_project" "$service_name" "$service_id" || exit 1
        service_state=$(docker_call inspect --format '{{.State.Status}}' "$service_id") || fail "Cannot inspect $service_name state."
        case "$service_state" in
            running) note "Reusing running $service_name." ;;
            created|exited)
                docker_call start "$service_id" >/dev/null || fail "Cannot start existing $service_name; check its logs and port bindings."
                note "Started existing $service_name."
                ;;
            *) fail "$service_name is $service_state; automatic recovery/recreation is disabled." ;;
        esac
    else
        validate_missing_service "$service_project" "$service_name" || exit 1
        ensure_image "$service_project" "$service_name" || exit 1
        # No dependencies, orphans, existing containers, or anonymous volumes are replaced here.
        compose 120 "$service_project" up -d --no-deps --no-recreate --no-build --pull never "$service_name" || \
            fail "Cannot create $service_name; inspect name/port conflicts and Compose settings."
        service_id=$(container_id "$service_project" "$service_name") || exit 1
        [ -n "$service_id" ] || fail "No container found after starting $service_name."
        validate_container "$service_project" "$service_name" "$service_id" || exit 1
        note "Created $service_name using its established Compose project."
    fi
)

# Resolve one actual loopback HTTP mapping, including configured port overrides.
# @param $1 Container ID. @param $2 Container port. @returns HTTP URL; fails for absent/ambiguous bindings.
http_address() (
    address_value=$(docker_call port "$1" "$2/tcp") || fail 'Cannot inspect published HTTP port.'
    printf '%s\n' "$address_value" | grep -E '^127\.0\.0\.1:[0-9]+$' >/dev/null || fail 'Expected one loopback HTTP binding.'
    [ "$(printf '%s\n' "$address_value" | wc -l | tr -d ' ')" = 1 ] || fail 'Multiple HTTP bindings are unsupported.'
    printf 'http://%s\n' "$address_value"
)

# Validate a worker-ready record belonging to its current container process without exposing logs.
# @param $1 Worker container ID. @returns 0 only for ready evidence and an unchanged running process.
worker_ready() (
    worker_id=$1
    worker_before=$(docker_call inspect --format '{{.State.Status}} {{.State.StartedAt}} {{.RestartCount}}' "$worker_id") || return 1
    set -- $worker_before
    [ "$#" -eq 3 ] && [ "$1" = running ] || return 1
    worker_since=$2
    worker_logs=$(docker_call logs --since "$worker_since" --tail 10000 "$worker_id" 2>&1) || return 1
    printf '%s\n' "$worker_logs" | docker_call exec -i "$worker_id" node --input-type=module -e '
// Check only structured records from this process; log contents never leave this validator.
let text = "";
for await (const chunk of process.stdin) text += chunk;
let ready = false;
for (const line of text.split("\n")) {
  let record;
  try { record = JSON.parse(line); } catch { continue; }
  if (record.component !== "reconciliation-worker") continue;
  if (record.event === "worker_ready" && record.outcome === "ready") ready = true;
  if (["worker_fatal", "worker_failed", "shutdown_started"].includes(record.event)) process.exit(1);
}
process.exit(ready ? 0 : 1);
' >/dev/null 2>&1 || return 1
    worker_after=$(docker_call inspect --format '{{.State.Status}} {{.State.StartedAt}} {{.RestartCount}}' "$worker_id") || return 1
    [ "$worker_before" = "$worker_after" ]
)

# Run one bounded readiness observation using only reads; no records or jobs are created.
# @param $1 Service. @param $2 Container ID. @returns 0 for readiness, nonzero for unavailable/invalid evidence.
probe() (
    probe_service=$1
    probe_id=$2
    probe_state=$(docker_call inspect --format '{{.State.Status}}' "$probe_id") || return 1
    [ "$probe_state" = running ] || return 1
    case "$probe_service" in
        postgres)
            docker_call exec "$probe_id" sh -c 'PGCONNECT_TIMEOUT=5 psql -X -w -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "SELECT 1"' >/dev/null 2>&1
            ;;
        mongo)
            docker_call exec "$probe_id" mongosh --quiet --eval 'quit(db.adminCommand({ping: 1}).ok === 1 ? 0 : 1)' >/dev/null 2>&1
            ;;
        update-api)
            probe_address=$(http_address "$probe_id" 43187) || return 1
            bounded 10 curl --noproxy '*' --fail --silent --max-time 5 "$probe_address/health/live" >/dev/null || return 1
            docker_call exec "$probe_id" node --input-type=module -e '
// Require both dependency observations, because HTTP 200 alone permits catalog degradation.
try {
  const response = await fetch("http://127.0.0.1:43187/health/ready", {signal: AbortSignal.timeout(5000)});
  const body = await response.json();
  process.exit(response.ok && body.ready === true && body.dependencies?.postgres?.available === true && body.dependencies?.mongodb?.available === true ? 0 : 1);
} catch { process.exit(1); }
' >/dev/null 2>&1
            ;;
        reconciliation-worker) worker_ready "$probe_id" ;;
        *)
            case "$probe_service" in
                grafana) probe_port=3000; probe_path=/api/health ;;
                loki) probe_port=3100; probe_path=/ready ;;
                alloy) probe_port=12345; probe_path=/-/healthy ;;
                *) return 1 ;;
            esac
            probe_address=$(http_address "$probe_id" "$probe_port") || return 1
            bounded 10 curl --noproxy '*' --fail --silent --max-time 5 "$probe_address$probe_path" >/dev/null
            ;;
    esac
)

# Wait at most two minutes for a service; check mode makes one observation and never waits for startup.
# @param $1 Project. @param $2 Service. @returns 0 on observed readiness; exits with a non-secret error otherwise.
wait_ready() (
    ready_project=$1
    ready_service=$2
    ready_deadline=$(($(date +%s) + 120))
    # Share the earlier deadline across the loop, lookup and every nested probe.
    if [ "$setup_deadline" -lt "$ready_deadline" ]; then ready_deadline=$setup_deadline; fi
    setup_deadline=$ready_deadline
    ready_id=$(container_id "$ready_project" "$ready_service") || exit 1
    [ -n "$ready_id" ] || fail "$ready_service is absent."
    while :; do
        if probe "$ready_service" "$ready_id"; then
            note "Verified readiness: $ready_service."
            return 0
        fi
        if [ "$setup_check" -eq 1 ] || [ "$(date +%s)" -ge "$ready_deadline" ]; then
            if [ "$ready_service" = reconciliation-worker ]; then
                fail 'Worker readiness is unconfirmed; inspect current-process logs and LOG_LEVEL=info. No restart was requested.'
            fi
            fail "$ready_service readiness failed; inspect its logs, connection settings, schema compatibility, and port bindings."
        fi
        sleep 2
    done
)

# Remove only the lock acquired by this invocation; never clean up Docker resources.
# @returns Preserves the caller exit status through the EXIT trap.
release_lock() {
    if [ "$setup_locked" -eq 1 ]; then
        rm -f "$setup_lock/pid"
        rmdir "$setup_lock" 2>/dev/null || :
    fi
}

# Print URLs obtained from the selected running containers, rather than assumed host ports.
# @returns Nonzero if a selected endpoint cannot be resolved.
print_endpoints() (
    for endpoint_service in grafana loki alloy; do
        case "$endpoint_service" in
            grafana) endpoint_port=3000 ;;
            loki) endpoint_port=3100 ;;
            alloy) endpoint_port=12345 ;;
        esac
        endpoint_id=$(container_id docker-observability "$endpoint_service") || exit 1
        endpoint_url=$(http_address "$endpoint_id" "$endpoint_port") || exit 1
        printf '%s: %s\n' "$endpoint_service" "$endpoint_url"
    done
    if [ "$setup_infrastructure" -eq 0 ]; then
        endpoint_id=$(container_id update-api update-api) || exit 1
        endpoint_url=$(http_address "$endpoint_id" 43187) || exit 1
        printf 'Update API: %s\n' "$endpoint_url"
    fi
)

note 'Checking prerequisites and the selected Docker engine.'
command -v docker >/dev/null 2>&1 || fail 'Install/start Docker Desktop for Mac or Rancher Desktop with Moby.'
command -v curl >/dev/null 2>&1 || fail 'curl is required.'
if [ -n "${COMPOSE_PROJECT_NAME-}${COMPOSE_FILE-}${COMPOSE_PROFILES-}${COMPOSE_ENV_FILES-}${DOCKER_HOST-}${DOCKER_DEFAULT_PLATFORM-}" ]; then
    fail 'Unset COMPOSE_PROJECT_NAME, COMPOSE_FILE, COMPOSE_PROFILES, COMPOSE_ENV_FILES, DOCKER_HOST, and DOCKER_DEFAULT_PLATFORM overrides before setup.'
fi
setup_version=$(docker_call compose version --short) || fail 'Docker Compose is unavailable.'
setup_version=${setup_version#v}
setup_major=${setup_version%%.*}
setup_minor_tail=${setup_version#*.}
setup_minor=${setup_minor_tail%%.*}
case "$setup_major:$setup_minor" in
    *[!0-9:]*|:*|*:) fail 'Cannot parse Docker Compose version.' ;;
esac
[ "$setup_major" -gt 2 ] || { [ "$setup_major" -eq 2 ] && [ "$setup_minor" -ge 30 ]; } || fail 'Docker Compose 2.30 or later is required.'
setup_context=$(docker_call context show) || fail 'Cannot read Docker context.'
setup_endpoint=$(docker_call context inspect "$setup_context" --format '{{.Endpoints.docker.Host}}') || fail 'Cannot inspect Docker context.'
case "$setup_endpoint" in unix://*) ;; *) fail 'Use a local Unix-socket Docker context on this Mac.' ;; esac
setup_os=$(docker_call info --format '{{.OSType}}') || fail 'Cannot reach Docker; check the daemon and selected context.'
[ "$setup_os" = linux ] || fail 'Docker must run Linux containers on this Mac.'
note "Using Docker context $setup_context (Compose $setup_version)."

setup_files='postGres/postGresCompose.yaml mongoDb/mongoCompose.yaml observability/compose.yaml observability/loki/config.yaml observability/alloy/config.alloy observability/grafana/provisioning/datasources/loki.yaml'
if [ "$setup_infrastructure" -eq 0 ]; then
    setup_files="$setup_files update-api/compose.yaml reconciliation-worker/compose.yaml"
fi
for setup_file in $setup_files; do
    [ -r "$setup_root/$setup_file" ] || fail "Missing required source: $setup_file."
done
setup_config_incomplete=0
configuration_file observability/.env || setup_config_incomplete=1
if [ "$setup_infrastructure" -eq 0 ]; then
    configuration_file update-api/.env.docker || setup_config_incomplete=1
    configuration_file reconciliation-worker/.env.docker || setup_config_incomplete=1
fi
[ "$setup_config_incomplete" -eq 0 ] || fail 'Configuration is incomplete. Review the listed private files and rerun the same mode.'
if [ "$setup_infrastructure" -eq 0 ]; then
    for setup_file in update-api/.env.docker reconciliation-worker/.env.docker; do
        if grep -E '^[[:space:]]*(POSTGRES_URL|MONGODB_URI)=.*(YOUR_USER|YOUR_PASSWORD)' "$setup_root/$setup_file" >/dev/null; then
            fail "Replace connection placeholders in $setup_file."
        fi
    done
fi
setup_projects='postgres mongodb docker-observability'
if [ "$setup_infrastructure" -eq 0 ]; then setup_projects="$setup_projects update-api reconciliation-worker"; fi
for setup_project in $setup_projects; do
    compose 30 "$setup_project" config --quiet >/dev/null 2>&1 || fail "Invalid $setup_project Compose configuration; check its private settings without exposing credentials."
done
note 'Selected Compose configurations validated.'

setup_networks=$(docker_call network ls --filter name=glue-local --format '{{.Name}}') || fail 'Cannot inventory Docker networks.'
setup_network_exists=0
for setup_network in $setup_networks; do
    if [ "$setup_network" = glue-local ]; then setup_network_exists=1; fi
done
if [ "$setup_network_exists" -eq 1 ]; then
    setup_network_settings=$(docker_call network inspect glue-local --format '{{.Driver}} {{.Scope}} {{.Internal}}') || fail 'Cannot inspect glue-local; no replacement will be created.'
    [ "$setup_network_settings" = 'bridge local false' ] || fail 'glue-local must be a local non-internal bridge.'
elif [ "$setup_check" -eq 1 ]; then
    fail 'glue-local is absent; check mode creates nothing.'
fi

# Inspect every existing selected container before changing any service lifecycle.
setup_services=$(services)
while read -r setup_project setup_service; do
    setup_id=$(container_id "$setup_project" "$setup_service") || exit 1
    if [ -n "$setup_id" ]; then
        validate_container "$setup_project" "$setup_service" "$setup_id" || exit 1
    elif [ "$setup_check" -eq 1 ]; then
        fail "$setup_service is absent; check mode starts nothing."
    else
        validate_missing_service "$setup_project" "$setup_service" || exit 1
    fi
done <<EOF
$setup_services
EOF

if [ "$setup_check" -eq 0 ]; then
    setup_lock_key=$(printf '%s' "$setup_endpoint" | cksum | awk '{print $1}')
    setup_lock="${TMPDIR:-/tmp}/retailer-docker-setup-$(id -u)-$setup_lock_key.lock"
    (umask 077; mkdir "$setup_lock") 2>/dev/null || fail "Another setup may own $setup_lock. Inspect a stale lock before retrying."
    setup_locked=1
    trap release_lock EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM HUP
    printf '%s\n' "$$" > "$setup_lock/pid"
    # Recheck under the lock; another invocation could have created the network during preflight.
    setup_networks=$(docker_call network ls --filter name=glue-local --format '{{.Name}}') || fail 'Cannot recheck Docker networks.'
    if ! printf '%s\n' "$setup_networks" | grep -Fx glue-local >/dev/null; then
        docker_call network create --driver bridge glue-local >/dev/null || fail 'Cannot create glue-local; inspect current state before retrying.'
        note 'Created glue-local.'
    fi
    setup_network_settings=$(docker_call network inspect glue-local --format '{{.Driver}} {{.Scope}} {{.Internal}}') || fail 'Cannot validate glue-local.'
    [ "$setup_network_settings" = 'bridge local false' ] || fail 'glue-local settings changed during setup.'
    mkdir -p "$setup_root/observability/grafana/dashboards" "$setup_root/observability/grafana/provisioning/dashboards"
fi

while read -r setup_project setup_service; do
    if [ "$setup_check" -eq 0 ]; then ensure_service "$setup_project" "$setup_service" || exit 1; fi
    wait_ready "$setup_project" "$setup_service" || exit 1
done <<EOF
$setup_services
EOF
print_endpoints
if [ "$setup_check" -eq 1 ]; then
    note 'Selected service checks passed; no configuration or container changes requested.'
elif [ "$setup_infrastructure" -eq 1 ]; then
    note 'Infrastructure is ready. Application services were not selected.'
else
    note 'All seven services passed readiness checks.'
fi
