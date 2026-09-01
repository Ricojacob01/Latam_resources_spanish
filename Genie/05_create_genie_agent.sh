#!/usr/bin/env bash
# Crea un Genie Agent desde un archivo serialized_space (genérico, reutilizable).
# Por defecto usa el generado por 04_build_genie_agent.py; pásale otro con AGENT_JSON=...
#
# Uso:
#   PROFILE=ucode WAREHOUSE_ID=... PARENT_PATH=/Users/tu@databricks.com/Latam_resources_spanish/Genie \
#   TITLE="Cliente X" AGENT_JSON=genie_agent.generated.json ./05_create_genie_agent.sh
#
# ¿No sabes el WAREHOUSE_ID?   databricks warehouses list --profile "$PROFILE"
set -euo pipefail

PROFILE="${PROFILE:-ucode}"
WAREHOUSE_ID="${WAREHOUSE_ID:-18479908b6be4949}"          # Serverless Starter Warehouse (cámbialo por cuenta)
PARENT_PATH="${PARENT_PATH:-/Users/rico.martinez@databricks.com/Latam_resources_spanish/Genie}"
TITLE="${TITLE:-Genie Agent (Taller)}"
DESC="${DESC:-Genie Agent del taller.}"
AGENT_JSON="${AGENT_JSON:-genie_agent.generated.json}"     # usa el generado; o genie_agent.json (ejemplo)
HERE="$(cd "$(dirname "$0")" && pwd)"

if [ ! -f "$HERE/$AGENT_JSON" ]; then
  echo "⛔ No encuentro $AGENT_JSON. Genera uno primero con 04_build_genie_agent.py"
  echo "   o usa el ejemplo:  CATALOG=<cat_compartido> AGENT_JSON=genie_agent.json ./05_create_genie_agent.sh"
  exit 1
fi

# Si el JSON es una plantilla con tokens (__CATALOG__/__SCHEMA__), localízalos.
# CATALOG lo pones tú; SCHEMA se deriva de tu usuario igual que en los notebooks.
SRC="$HERE/$AGENT_JSON"
if grep -q '__CATALOG__\|__SCHEMA__' "$SRC"; then
  : "${CATALOG:?Define CATALOG=<catálogo compartido> para localizar la plantilla $AGENT_JSON}"
  if [ -z "${SCHEMA:-}" ]; then
    EMAIL=$(databricks current-user me --profile "$PROFILE" -o json | jq -r '.emails[] | select(.primary==true) | .value')
    SCHEMA="taller_genie_$(echo "${EMAIL%@*}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
  fi
  SRC="$(mktemp)"
  sed -e "s/__CATALOG__/${CATALOG}/g" -e "s/__SCHEMA__/${SCHEMA}/g" "$HERE/$AGENT_JSON" > "$SRC"
  echo "🔧 Plantilla localizada: __CATALOG__→${CATALOG}, __SCHEMA__→${SCHEMA}"
fi

databricks workspace mkdirs "$PARENT_PATH" --profile "$PROFILE"

databricks genie create-space --profile "$PROFILE" --json "{
  \"warehouse_id\": \"$WAREHOUSE_ID\",
  \"title\": \"$TITLE\",
  \"description\": \"$DESC\",
  \"parent_path\": \"$PARENT_PATH\",
  \"serialized_space\": $(cat "$SRC" | jq -c '.' | jq -Rs '.')
}"

echo "✅ Listo. Lista los espacios:  databricks genie list-spaces --profile $PROFILE"
echo "   Luego puntúa con:  python3 06_benchmark_agent.py --profile $PROFILE --space-id <ID> --benchmarks benchmarks.csv"
