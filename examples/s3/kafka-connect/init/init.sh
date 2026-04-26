#!/bin/sh
set -eu

for file in /init/*.json; do
  [ -e "$file" ] || exit 0

  name="$(basename "$file" .json)"
  echo "Applying connector: $name"

  curl -fsS -X PUT \
    "$CONNECT_URL/connectors/$name/config" \
    -H "Content-Type: application/json" \
    --data @"$file"

  echo
done

echo "Init done"
