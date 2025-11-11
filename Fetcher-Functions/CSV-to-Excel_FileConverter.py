import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Alignment, Border, Side
from openpyxl.worksheet.table import Table, TableStyleInfo
from pathlib import Path
from openpyxl.utils import get_column_letter

# === PERCORSI INPUT/OUTPUT ===
files = {
    r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\github_combined.csv":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_github.xlsx",

    r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\lodcloud_results.csv":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_lodcloud.xlsx",

    r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\zenodo_combined.csv":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_zenodo.xlsx",

    r"C:\Users\maria\Desktop\Replication package\Biblioteca\Biblioteca.csv":
        r"C:\Users\maria\Desktop\Replication package\File Excel\output_zotero.xlsx",
}

# === FUNZIONE DI FORMATTAZIONE ===

def format_excel_table(excel_path):
    wb = load_workbook(excel_path)
    ws = wb.active

    # Range automatico per la tabella (usa get_column_letter!)
    max_row = ws.max_row
    max_col = ws.max_column
    last_col = get_column_letter(max_col)
    table_range = f"A1:{last_col}{max_row}"

    # Crea tabella
    table = Table(displayName="DataTable", ref=table_range)
    style = TableStyleInfo(
        name="TableStyleMedium9",
        showRowStripes=True,
        showColumnStripes=False
    )
    table.tableStyleInfo = style
    ws.add_table(table)

    # Formatta celle: bordo + wrap text
    thin = Side(border_style="thin", color="000000")
    border = Border(top=thin, left=thin, right=thin, bottom=thin)

    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            cell.border = border

    # Adatta larghezza colonne
    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        adjusted_width = min(max_length + 2, 60)
        ws.column_dimensions[col_letter].width = adjusted_width

    wb.save(excel_path)
    print(f"🎨 Formattato Excel come tabella: {excel_path}")

# === PIPELINE ===
for csv_path, out_xlsx in files.items():
    print(f"📥 Leggo: {csv_path}")
    df = pd.read_csv(csv_path, encoding='utf-8', sep=None, engine='python')
    out_dir = Path(out_xlsx).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_xlsx, index=False)
    format_excel_table(out_xlsx)

print("\n✅ Tutti i file Excel sono stati creati e formattati con tabelle grafiche!")
