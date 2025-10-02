#!/usr/bin/env bash
set -euo pipefail

echo "⏳ Esperando Prefect API..."
until [ "$(curl -s -o /dev/null -w '%{http_code}' http://prefect-server:4200/api/health)" = "200" ]; do
  sleep 2
done
echo "✅ Prefect API OK"

echo "⚙️  Asegurando work pool 'default'..."
prefect work-pool inspect default >/dev/null 2>&1 || prefect work-pool create default --type process

echo "📦 Registrando deployments (watcher + etl)..."
python /app/prefect_flows/etl_deployment.py

echo "✅ Bootstrap listo."