#!/usr/bin/env bash
# Raises the broker's disk-usage guard so it refuses a publish rather than
# blocking one. Safe to run twice; waits for the broker before and after.
set -euo pipefail

service="${ARTEMIS_SERVICE:-artemis}"
config=/var/lib/artemis-instance/etc/broker.xml
timeout_seconds="${ARTEMIS_INIT_TIMEOUT:-240}"

live_count() {
  docker compose logs "$service" 2>/dev/null | grep -c "AMQ241004" || true
}

give_up() {
  echo "$1" >&2
  docker compose logs --tail 40 --no-color "$service" >&2 || true
  exit 1
}

wait_for_config() {
  local deadline=$((SECONDS + timeout_seconds))
  until docker compose exec -T "$service" test -f "$config" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || give_up "$service never wrote $config"
    sleep 2
  done
}

wait_for_live() {
  local was="$1"
  local deadline=$((SECONDS + timeout_seconds))
  until [[ "$(live_count)" -gt "$was" ]]; do
    ((SECONDS < deadline)) || give_up "$service did not come back up"
    sleep 2
  done
}

wait_for_config

if docker compose exec -T "$service" grep -q "<max-disk-usage>100</max-disk-usage>" "$config"; then
  echo "the disk guard on $service is already raised"
  exit 0
fi

before="$(live_count)"

docker compose exec -T "$service" \
  sed -i 's|<max-disk-usage>[0-9]*</max-disk-usage>|<max-disk-usage>100</max-disk-usage>|' "$config"

docker compose restart "$service"
wait_for_live "$before"

echo "the disk guard on $service is raised"
