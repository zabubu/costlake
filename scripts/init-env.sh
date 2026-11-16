#!/usr/bin/env sh
# Create .env from .env.example with random secrets. Refuses to overwrite.
set -eu
cd "$(dirname "$0")/.."
if [ -f .env ]; then
  echo ".env already exists; delete it first to regenerate secrets." >&2
  exit 1
fi
umask 077
while IFS= read -r line; do
  case "$line" in
    *=change-me*)
      key="${line%%=*}"
      # 32 alphanumeric characters: safe in URLs (database DSNs) and JSON.
      value="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32)"
      echo "${key}=${value}"
      ;;
    *) echo "$line" ;;
  esac
done < .env.example > .env
echo "wrote .env (mode 600)"
