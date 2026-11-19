#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
: "${TARASHA_RESEARCH_SNAPSHOT:?Configure the verified Research snapshot path}"
export TARASHA_READ_ONLY=1
export PYTHONDONTWRITEBYTECODE=1

exec python -m streamlit run app.py \
  --server.address=0.0.0.0 \
  --server.port="${PORT:-8000}" \
  --server.headless=true \
  --server.baseUrlPath="" \
  --server.enableCORS=true \
  --server.enableXsrfProtection=true \
  --server.corsAllowedOrigins=https://www.research.tarasha.biz \
  --browser.serverAddress=www.research.tarasha.biz \
  --browser.serverPort=443 \
  --browser.gatherUsageStats=false
