import json
import re
import sys
from typing import Dict, List, Any


REPLACEMENTS: Dict[str, str] = {
    # títulos y comunes
    r"\bGovernance Day\b": "Día de Gobernanza",
    r"\bDatabricks Brazil\b": "Databricks Brasil",
    r"\bAgenda\b": "Agenda",
    r"\bBem vindos!": "¡Bienvenidos!",
    r"\bEncerramento\b": "Cierre",
    r"\bMão na massa\b": "Manos a la obra",
    r"\bHistória de Sucesso\b": "Caso de éxito",
    r"\bClassificação de dados\b": "Clasificación de datos",
    r"\bComo\b": "Cómo",
    r"\bGenAI\b": "GenAI",
    r"\bGovernança\b": "Gobernanza",
    r"\bGovernança de Dados\b": "Gobernanza de Datos",
    r"\bGovernança de IA\b": "Gobernanza de IA",
    r"\bGovernança de Dados & IA\b": "Gobernanza de Datos e IA",
    r"\bOportunidades na\b": "Oportunidades en la",
    r"\bEcossistema Unificado\b": "Ecosistema unificado",
    r"\bPráticas padronizadas\b": "Prácticas estandarizadas",
    r"\bSegurança & Compliance\b": "Seguridad y cumplimiento",
    # vocabulario frecuente
    r"\bDados\b": "Datos",
    r"\bdados\b": "datos",
    r"\bempresa\b": "empresa",
    r"\bempresas\b": "empresas",
    r"\bframework\b": "marco",
    r"\bFramework\b": "Marco",
    r"\bprocessos\b": "procesos",
    r"\bpolíticas\b": "políticas",
    r"\bpadrões\b": "estándares",
    r"\bcoletado\b": "recopilado",
    r"\barmazenado\b": "almacenado",
    r"\bprotegido\b": "protegido",
    r"\butilizado\b": "utilizado",
    r"\bdisponibilidade\b": "disponibilidad",
    r"\busabilidade\b": "usabilidad",
    r"\bintegridade\b": "integridad",
    r"\bsegurança\b": "seguridad",
    r"\bregras\b": "reglas",
    r"\brequ\[ií\]sitos\b": "requisitos",
    r"\baderência\b": "adhesión",
    r"\bregulações\b": "regulaciones",
    r"\basset\b": "activo",
    r"\bética\b": "ética",
    r"\btransparente\b": "transparente",
    r"\bprivacidade\b": "privacidad",
    r"\bresiliência\b": "resiliencia",
    r"\binteroperabilidade\b": "interoperabilidad",
    r"\bdefinições\b": "definiciones",
    r"\bfonte única de verdade\b": "fuente única de verdad",
    r"\bmelhor\b": "mejor",
    r"\bameaças\b": "amenazas",
    r"\bconformidade\b": "conformidad",
    r"\bexplicabilidade\b": "explicabilidad",
    r"\bgestão\b": "gestión",
    r"\bqualidade\b": "calidad",
    r"\bcomplexidade\b": "complejidad",
    r"\bvantagem\b": "ventaja",
    r"\bconfiável\b": "confiable",
    r"\bvalor\b": "valor",
    r"\búnico\b": "único",
    r"\bsistemas\b": "sistemas",
    r"\bdecisões\b": "decisiones",
    r"\bmelhor tomada de decisões\b": "mejor toma de decisiones",
    # meses/días/comunes suelen ya coincidir o no requieren cambio
}


def pt_to_es(text: str) -> str:
    result = text
    for pattern, repl in REPLACEMENTS.items():
        result = re.sub(pattern, repl, result, flags=re.IGNORECASE)
    # Arreglos ortográficos comunes (atentos al orden)
    fixes = [
        ("ção", "ción"),
        ("ções", "ciones"),
        ("sões", "siones"),
        ("são", "sión"),
        ("ção", "ción"),
        ("ç", "c"),
        ("á", "á"), ("é", "é"), ("í", "í"), ("ó", "ó"), ("ú", "ú"),  # mantener acentos
    ]
    for src, dst in fixes:
        result = result.replace(src, dst)
    # Espacios múltiples -> simples
    result = re.sub(r"[ \t]+", " ", result)
    # Normaliza comillas
    result = result.replace("“", "«").replace("”", "»")
    return result


def translate_file(in_json: str, out_json: str):
    with open(in_json, "r", encoding="utf-8") as f:
        entries: List[dict] = json.load(f)
    out: List[dict] = []
    for e in entries:
        t = e.get("text") or ""
        es = pt_to_es(t)
        ne = dict(e)
        ne["translated_text"] = es
        out.append(ne)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Guardado: {out_json} ({len(out)} entradas)")


def main():
    if len(sys.argv) < 3:
        print("Uso: python tools/pt_to_es_rules.py <in.json> <out.json>")
        sys.exit(1)
    translate_file(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()


