#!/bin/sh
set -e
docker compose exec -T artemis sh -c \
  "sed -i 's|<max-disk-usage>[0-9]*</max-disk-usage>|<max-disk-usage>100</max-disk-usage>|' \
   /var/lib/artemis-instance/etc/broker.xml"
docker compose restart artemis
