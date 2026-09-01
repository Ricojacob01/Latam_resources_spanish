#!/usr/bin/env python3
"""
Puntúa un Genie Agent contra tu batería de benchmarks — SIN Genie Workbench.
Corre cada pregunta del CSV vía la Conversation API, captura el SQL que Genie generó
y su respuesta en texto, y escribe un CSV de resultados listo para marcar pass/fail.

Uso:
  python3 06_benchmark_agent.py --profile ucode --space-id <SPACE_ID> \\
      --benchmarks benchmarks.csv --out resultados.csv

Consigue el SPACE_ID con:  databricks genie list-spaces --profile <PERFIL>
Límite de Genie: ~5 preguntas por minuto → el script espera entre preguntas (--sleep).
"""
import argparse, csv, json, subprocess, sys, time


def cli(args, check=True):
    r = subprocess.run(["databricks", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"CLI error: {' '.join(args)}\n{r.stderr}")
    return r.stdout, r.returncode, r.stderr


def ask(profile, space_id, question, poll_timeout, poll_every):
    """Lanza una pregunta y espera el resultado. Devuelve (status, sql, texto, error)."""
    out, rc, err = cli(["genie", "start-conversation", "--no-wait", "--profile", profile,
                        space_id, question], check=False)
    if rc != 0:
        return "START_FAILED", "", "", err.strip()
    ids = json.loads(out)
    conv, msg = ids["conversation_id"], ids["message_id"]

    deadline = time.time() + poll_timeout
    state = "UNKNOWN"
    while time.time() < deadline:
        out, rc, err = cli(["genie", "get-message", "--profile", profile, space_id, conv, msg], check=False)
        if rc != 0:
            return "POLL_FAILED", "", "", err.strip()
        m = json.loads(out)
        state = m.get("status", "UNKNOWN")
        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            sql, text = "", ""
            for att in m.get("attachments", []) or []:
                q = (att.get("query") or {})
                if q.get("query"):
                    sql = q["query"]
                t = (att.get("text") or {})
                if t.get("content"):
                    text = t["content"]
            error = (m.get("error") or {}).get("error", "")
            return state, sql, text, error
        time.sleep(poll_every)
    return f"TIMEOUT ({state})", "", "", ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--space-id", required=True)
    ap.add_argument("--benchmarks", default="benchmarks.csv")
    ap.add_argument("--out", default="resultados_benchmark.csv")
    ap.add_argument("--question-col", default="pregunta", help="columna del CSV con la pregunta")
    ap.add_argument("--sleep", type=float, default=13.0, help="segundos entre preguntas (límite ~5/min)")
    ap.add_argument("--poll-timeout", type=int, default=120)
    ap.add_argument("--poll-every", type=float, default=3.0)
    a = ap.parse_args()

    with open(a.benchmarks, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        sys.exit(f"{a.benchmarks} está vacío.")
    if a.question_col not in rows[0]:
        sys.exit(f"El CSV no tiene la columna '{a.question_col}'. Columnas: {list(rows[0].keys())}")

    results = []
    total = len([r for r in rows if r.get(a.question_col, "").strip()])
    i = 0
    for r in rows:
        q = r.get(a.question_col, "").strip()
        if not q:
            continue
        i += 1
        print(f"[{i}/{total}] ({r.get('tier','')}) {q[:70]}...", flush=True)
        status, sql, text, error = ask(a.profile, a.space_id, q, a.poll_timeout, a.poll_every)
        print(f"    → {status}" + (f"  · {len(sql)} chars de SQL" if sql else "  · sin SQL")
              + (f"  · error: {error[:80]}" if error else ""), flush=True)
        results.append({
            "id": r.get("id", ""),
            "tier": r.get("tier", ""),
            "pregunta": q,
            "status": status,
            "sql_generado": sql,
            "texto_respuesta": text,
            "error": error,
            "pass_fail": "",   # ← llénalo tú: pass / fail
            "notas": r.get("notas", ""),
        })
        if i < total:
            time.sleep(a.sleep)

    cols = ["id", "tier", "pregunta", "status", "sql_generado", "texto_respuesta", "error", "pass_fail", "notas"]
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(results)

    completed = sum(1 for x in results if x["status"] == "COMPLETED")
    tier1 = [x for x in results if x["tier"] == "tier-1"]
    tier1_ok = sum(1 for x in tier1 if x["status"] == "COMPLETED" and x["sql_generado"])
    print(f"\n✅ Escrito {a.out}")
    print(f"   Completadas (Genie respondió): {completed}/{len(results)}")
    if tier1:
        print(f"   tier-1 con SQL generado: {tier1_ok}/{len(tier1)}  (meta ≥ 85% tras marcar pass/fail)")
    print("   → Abre el CSV, marca pass/fail revisando el sql_generado, y afina el agente en lo que falle.")


if __name__ == "__main__":
    main()
