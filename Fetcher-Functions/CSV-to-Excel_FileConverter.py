import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Side
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter
from pathlib import Path
import csv
import bibtexparser

# === PERCORSI INPUT/OUTPUT ===
files = {
    r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\github_combined.csv":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_github.xlsx",

    r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\lodcloud_results.csv":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_lodcloud.xlsx",

    # r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\zenodo_combined.csv":  # disattivato CSV
    #     r"C:\Users\maria\Desktop\Replication package\File Excel\output_zenodo.xlsx",

    r"C:\Users\maria\Desktop\Replication package\Biblioteca\Biblioteca.csv":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_zotero.xlsx",
}

zenodo_bib = r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\zenodo_combined.bib"
zenodo_xlsx = r"C:\Users\maria\Desktop\Replication package\File Excel\output_zenodo.xlsx"


# === Lettura CSV standard ===
def detect_separator(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sample = f.read(4096)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t'])
            return dialect.delimiter
        except Exception:
            return ','


def read_csv_stable(file_path):
    sep = detect_separator(file_path)
    print(f"[INFO] Rilevato separatore '{sep}' per {file_path}")
    df = pd.read_csv(file_path, encoding='utf-8', sep=sep, on_bad_lines='skip')
    print(f"[INFO] Letto CSV: {len(df)} righe, {len(df.columns)} colonne")
    return df


# === Lettura diretta del file .bib di Zenodo ===
def read_zenodo_bib(bib_path):
    print(f"[INFO] Leggo BibTeX: {bib_path}")
    with open(bib_path, encoding='utf-8') as bibtex_file:
        bib_database = bibtexparser.load(bibtex_file)

    entries = []
    for entry in bib_database.entries:
        entries.append({
            "Title": entry.get("title", "").replace("\n", " ").strip(),
            "Author": entry.get("author", "").replace("\n", " ").strip(),
            "Year": entry.get("year", ""),
            "DOI": entry.get("doi", ""),
            "URL": entry.get("url", ""),
            "Abstract": entry.get("abstract", "").replace("\n", " ").strip(),
            "Publisher": entry.get("publisher", entry.get("journal", "")),
            "EntryType": entry.get("ENTRYTYPE", ""),
        })

    df = pd.DataFrame(entries)
    print(f"[OK] Letto BibTeX Zenodo: {len(df)} record, {len(df.columns)} colonne")
    return df


# === Formattazione Excel ===
def format_excel_table(excel_path):
    wb = load_workbook(excel_path)
    ws = wb.active

    max_row = ws.max_row
    max_col = ws.max_column
    last_col = get_column_letter(max_col)
    table_range = f"A1:{last_col}{max_row}"

    table = Table(displayName="DataTable", ref=table_range)
    style = TableStyleInfo(name="TableStyleMedium9", showRowStripes=True)
    table.tableStyleInfo = style
    ws.add_table(table)

    thin = Side(border_style="thin", color="000000")
    border = Border(top=thin, left=thin, right=thin, bottom=thin)

    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            cell.border = border

    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = min(max_length + 2, 60)

    wb.save(excel_path)
    print(f"🎨 Formattato Excel come tabella: {excel_path}")


# === Pipeline principale ===
for csv_path, out_xlsx in files.items():
    print(f"\n📥 Elaboro: {csv_path}")
    df = read_csv_stable(csv_path)

    # 🔹 Rimuovi colonne completamente vuote
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        print(f"[CLEANUP] Rimosse colonne vuote: {empty_cols}")
        df = df.drop(columns=empty_cols)

    # 🔹 Ordina se possibile
    date_col = next((c for c in df.columns if any(k in c.lower() for k in ['date', 'time', 'year'])), None)
    name_col = next((c for c in df.columns if any(k in c.lower() for k in ['title', 'name'])), None)

    if date_col:
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.tz_localize(None)
            df['Year'] = df[date_col].dt.year
            sort_cols = ['Year', date_col]
            ascending = [False, False]
            if name_col:
                sort_cols.append(name_col)
                ascending.append(True)
            df = df.sort_values(by=sort_cols, ascending=ascending).drop(columns=['Year'])
        except Exception as e:
            print(f"[WARN] Ordinamento non riuscito per {csv_path}: {e}")

    out_dir = Path(out_xlsx).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_xlsx, index=False)
    format_excel_table(out_xlsx)

# === Zenodo dal .bib ===
print("\n📚 Elaboro Zenodo dal .bib ...")
df_zen = read_zenodo_bib(zenodo_bib)

# Ordina per anno decrescente + titolo
if 'Year' in df_zen.columns:
    try:
        df_zen['Year'] = pd.to_numeric(df_zen['Year'], errors='coerce')
        df_zen = df_zen.sort_values(by=['Year', 'Title'], ascending=[False, True])
    except Exception:
        pass

df_zen.to_excel(zenodo_xlsx, index=False)
format_excel_table(zenodo_xlsx)

print("\n✅ Tutti i file Excel (incluso Zenodo da BibTeX) sono stati creati e formattati correttamente!")
