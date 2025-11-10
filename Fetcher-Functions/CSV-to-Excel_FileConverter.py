import re
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

# ---- PULIZIA HTML ----
def clean_html(text):
    if not isinstance(text, str):
        return text
    
    # Rimuovi HTML
    text = BeautifulSoup(text, "lxml").get_text(separator=" ", strip=True)
    
    # Collassa whitespace
    text = re.sub(r"\s+", " ", text)
    
    # Rimuovi spazi prima di punteggiatura
    text = re.sub(r"\s+([,.])", r"\1", text)

    return text.strip()

# ---- PARSER BIBTEX RAW ----
def parse_bibtex_raw(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        raw = f.read()

    raw_entries = re.split(r'(?=@\w+{)', raw)
    entries = []
    key_order = []  # per salvare ordine campi del primo record

    for block in raw_entries:
        block = block.strip()
        if not block:
            continue

        m = re.match(r'@(\w+){([^,]+),', block)
        if not m:
            continue

        entrytype, entry_id = m.groups()
        entry_dict = {"ENTRYTYPE": entrytype, "ID": entry_id}

        field_pattern = r'(\w+)\s*=\s*(\{(?:[^{}]|\{[^{}]*\})*\}|".*?")'
        fields = re.findall(field_pattern, block, flags=re.DOTALL)

        if not key_order:
            key_order = ["ENTRYTYPE", "ID"] + [f[0].lower() for f in fields]

        for key, val in fields:
            val = val.strip()
            if (val.startswith("{") and val.endswith("}")) or (val.startswith('"') and val.endswith('"')):
                val = val[1:-1]

            val = re.sub(r'\\url\{(.*?)\}', r'\1', val)  # estrai URL dalla \url{}

            entry_dict[key.lower()] = clean_html(val)  # pulizia HTML qui ✅

        entries.append(entry_dict)

    # Mantieni ordine originale + eventuali colonne extra alla fine
    df = pd.DataFrame(entries)
    ordered_cols = [c for c in key_order if c in df.columns] + [c for c in df.columns if c not in key_order]
    df = df[ordered_cols]

    return df

# ---- FILES ----
files = {
    r"C:\Users\maria\Desktop\ReplicableSLR\pybibx\fetcher\github_combined.bib": 
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_github.xlsx",

    r"C:\Users\maria\Desktop\ReplicableSLR\pybibx\fetcher\lodcloud_results.bib":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_lodcloud.xlsx",

    r"C:\Users\maria\Desktop\Replication package\Biblioteca\Biblioteca.bib":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_zotero.xlsx"
}

# ---- RUN ----
for bib, out in files.items():
    print(f"📥 Leggo: {bib}")
    df = parse_bibtex_raw(bib)
    df.to_excel(out, index=False)
    print(f"✅ Creato Excel: {out}")

print("\n🎉 COMPLETATO — Colonne in ordine BibTeX, HTML pulito, tutto leggibile.")
