import json
import sys
import time
from typing import List, Dict, Any

try:
    # MyMemory no requiere API key; sujeto a límites de uso.
    from deep_translator import MyMemoryTranslator
except Exception as e:
    print("Falta dependencia deep_translator. Instálala antes de ejecutar.")
    raise


def translate_entries(in_json: str, out_json: str, src_lang: str = "pt", tgt_lang: str = "es", delay_s: float = 0.2):
    with open(in_json, "r", encoding="utf-8") as f:
        entries: List[Dict[str, Any]] = json.load(f)

    translator = MyMemoryTranslator(source=src_lang, target=tgt_lang)
    translated: List[Dict[str, Any]] = []
    for idx, entry in enumerate(entries, start=1):
        text = entry.get("text", "")
        t_text = text
        if text and text.strip():
            try:
                # Traduce por bloques pequeños; si hay saltos de línea, mantenlos
                parts = text.split("\n")
                t_parts = []
                for p in parts:
                    if p.strip():
                        t_parts.append(translator.translate(p))
                        time.sleep(delay_s)
                    else:
                        t_parts.append(p)
                t_text = "\n".join(t_parts)
            except Exception:
                # Si falla, conserva original
                t_text = text
        new_entry = dict(entry)
        new_entry["translated_text"] = t_text
        translated.append(new_entry)
        if idx % 50 == 0:
            print(f"Progreso: {idx}/{len(entries)} traducidos...")

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(translated, f, ensure_ascii=False, indent=2)
    print(f"Guardado: {out_json} ({len(translated)} entradas).")


def main():
    if len(sys.argv) < 3:
        print("Uso: python tools/translate_json.py <in.json> <out.json> [src_lang] [tgt_lang]")
        sys.exit(1)
    in_json = sys.argv[1]
    out_json = sys.argv[2]
    src = sys.argv[3] if len(sys.argv) > 3 else "pt"
    tgt = sys.argv[4] if len(sys.argv) > 4 else "es"
    translate_entries(in_json, out_json, src_lang=src, tgt_lang=tgt)


if __name__ == "__main__":
    main()


