#!/usr/bin/env python3
"""
Genera un genie_agent.json (serialized_space) a partir de CUALQUIER conjunto de tablas
de Unity Catalog, introspeccionando columnas y comentarios vía la CLI de Databricks.
Reutilizable por cliente — no hay nada hardcodeado.

Patrón multi-usuario (catálogo compartido + esquema por usuario):
  Usa --auto-user-schema para derivar tu esquema igual que los notebooks
  (taller_genie_<usuario>), sin tener que escribirlo.

Ejemplos:
  # Esquema personal del taller, derivado automáticamente del usuario logueado
  python3 04_build_genie_agent.py --profile ucode --catalog CAT_COMPARTIDO --auto-user-schema

  # Esquema explícito, todas sus tablas
  python3 04_build_genie_agent.py --profile ucode --catalog CAT --schema SCH

  # Tablas específicas + crear el espacio directamente
  python3 04_build_genie_agent.py --profile ucode --catalog CAT --schema SCH \\
      --tables gold_oportunidades --warehouse 18479908b6be4949 \\
      --parent-path /Users/tu@databricks.com/Latam_resources_spanish/Genie \\
      --title "Cliente X" --create
"""
import argparse, json, re, subprocess, sys


def cli(args):
    r = subprocess.run(["databricks", *args], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"CLI error: {' '.join(args)}\n{r.stderr}")
    return r.stdout


def current_username(profile):
    out = cli(["current-user", "me", "--profile", profile, "-o", "json"])
    emails = json.loads(out).get("emails", [])
    email = next((e["value"] for e in emails if e.get("primary")), emails[0]["value"] if emails else "")
    if not email:
        sys.exit("No pude determinar el usuario actual para --auto-user-schema.")
    return email


def derive_schema(email):
    handle = email.split("@")[0].lower()
    return "taller_genie_" + re.sub(r"[^a-z0-9]+", "_", handle).strip("_")


def list_tables(profile, catalog, schema):
    out = cli(["tables", "list", catalog, schema, "--profile", profile, "-o", "json"])
    return [t["name"] for t in (json.loads(out) if out.strip() else [])]


def get_columns(profile, fq):
    out = cli(["tables", "get", fq, "--profile", profile, "-o", "json"])
    meta = json.loads(out)
    cols = []
    for c in meta.get("columns", []):
        cols.append({"name": c["name"],
                     "type": c.get("type_text") or c.get("type_name", ""),
                     "comment": c.get("comment", "")})
    return cols, meta.get("comment", "")


def hid(prefix, n):  # 32-char hex id
    return f"{prefix}{n:031d}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="", help="Esquema; omítelo y usa --auto-user-schema para derivarlo")
    ap.add_argument("--auto-user-schema", action="store_true",
                    help="Deriva el esquema del usuario logueado: taller_genie_<usuario>")
    ap.add_argument("--tables", default="", help="CSV; vacío = todas las del esquema")
    ap.add_argument("--warehouse", default="")
    ap.add_argument("--parent-path", default="")
    ap.add_argument("--title", default="Genie Agent (Taller)")
    ap.add_argument("--description", default="Genie Agent generado desde Unity Catalog.")
    ap.add_argument("--out", default="genie_agent.generated.json")
    ap.add_argument("--create", action="store_true", help="crear el espacio tras generar el JSON")
    a = ap.parse_args()

    schema = a.schema.strip()
    if a.auto_user_schema and not schema:
        schema = derive_schema(current_username(a.profile))
        print(f"[auto] esquema derivado: {schema}")
    if not schema:
        sys.exit("Falta --schema (o usa --auto-user-schema).")

    tables = [t.strip() for t in a.tables.split(",") if t.strip()] or list_tables(a.profile, a.catalog, schema)
    if not tables:
        sys.exit(f"No se encontraron tablas en {a.catalog}.{schema}.")

    identifiers, schema_lines, num_cols, cat_hints = [], [], [], []
    for t in sorted(tables):
        fq = f"{a.catalog}.{schema}.{t}"
        identifiers.append(fq)
        cols, tcomment = get_columns(a.profile, fq)
        schema_lines.append(f"- {t}: " + (tcomment or "(sin descripción)"))
        for c in cols:
            schema_lines.append(f"    · {c['name']} ({c['type']}) {('- ' + c['comment']) if c['comment'] else ''}")
            low = c["type"].lower()
            if any(k in low for k in ["int", "double", "decimal", "float", "long"]):
                num_cols.append((t, c["name"]))
            if c["name"].lower().endswith(("_id", "_key", "id")):
                cat_hints.append(c["name"])

    # sample questions genéricas
    sample = [
        {"id": hid("1", 1), "question": ["¿Cuántos registros hay en total?"]},
        {"id": hid("1", 2), "question": ["Muéstrame 10 filas de ejemplo de la tabla principal."]},
    ]
    if num_cols:
        t, n = num_cols[0]
        sample.append({"id": hid("1", 3), "question": [f"¿Cuál es la suma y el promedio de {n}?"]})
    sample.append({"id": hid("1", 4), "question": ["¿Cuáles son las tendencias o los totales más importantes en estos datos?"]})

    # un ejemplo SQL genérico (conteo) para satisfacer la estructura
    first = sorted(identifiers)[0]
    example_sqls = [{
        "id": hid("2", 1),
        "question": ["¿Cuántos registros hay en total?"],
        "sql": [f"SELECT COUNT(*) AS total FROM {first}"]
    }]

    instr = [
        "Eres un asistente de analítica de datos. Responde en español, claro y conciso.\n\n",
        "TABLAS Y ESQUEMA DISPONIBLE:\n",
        *[l + "\n" for l in schema_lines],
        "\nCOMPLETAR EN EL TALLER (con el champion):\n",
        "- DEFINICIONES DE NEGOCIO: cómo se calcula cada métrica clave (fórmula + tabla/columna).\n",
        "- CLAVES DE UNIÓN entre tablas (candidatas detectadas: " + (", ".join(sorted(set(cat_hints))) or "revisar") + ").\n",
        "- SINÓNIMOS: mapea los términos del usuario a los valores reales de las columnas.\n",
        "- Periodo por defecto cuando el usuario no lo especifique.\n",
    ]
    text_instructions = [{"id": hid("3", 1), "content": instr}]

    space = {
        "version": 2,
        "config": {"sample_questions": sample},
        "data_sources": {"tables": [{"identifier": i} for i in sorted(identifiers)]},
        "instructions": {
            "example_question_sqls": sorted(example_sqls, key=lambda x: x["id"]),
            "text_instructions": text_instructions,
        },
    }
    with open(a.out, "w") as f:
        json.dump(space, f, ensure_ascii=False, indent=2)
    print(f"Escrito {a.out} con {len(identifiers)} tablas de {a.catalog}.{schema}.")

    if a.create:
        if not (a.warehouse and a.parent_path):
            sys.exit("--create requiere --warehouse y --parent-path")
        cli(["workspace", "mkdirs", a.parent_path, "--profile", a.profile])
        payload = {"warehouse_id": a.warehouse, "title": a.title,
                   "description": a.description, "parent_path": a.parent_path,
                   "serialized_space": json.dumps(space, ensure_ascii=False)}
        out = cli(["genie", "create-space", "--profile", a.profile, "--json", json.dumps(payload)])
        print(out)


if __name__ == "__main__":
    main()
