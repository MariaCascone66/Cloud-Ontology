import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# --- Fetchers ---
from Scopus_fetcher import ScopusFetcher
from github_multifetcher_filtered import GitHubFetcher
from Zenodo_fetcher import ZenodoFetcher
from LodCloud_fetcher import LodCloudFetcher


def main():

    # =====================================================
    # CARTELLA RISULTATI → quella che hai chiesto
    # =====================================================
    output_dir = r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results"
    os.makedirs(output_dir, exist_ok=True)

    # =====================================================
    # ===================== SCOPUS ========================
    # =====================================================
    load_dotenv(r"C:\Users\maria\Desktop\Cloud-Ontology\scopus_key.env")
    SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY")

    if not SCOPUS_API_KEY:
        raise ValueError("⚠️ SCOPUS_API_KEY non trovata nel .env")

    scopus = ScopusFetcher(SCOPUS_API_KEY)

    # === 🔥 QUERY IDENTICA ALLA TUA ORIGINALE (841 risultati) ===
    query = (
        'TITLE-ABS-KEY ( ( "cloud computing" OR "cloud-computing" OR "multi-cloud" ) '
        'AND ( "ontolog*" OR "semantic web" OR "knowledge graph*" OR "linked data" OR "linked open data" ) '
        'AND NOT ( "internet of things" OR "iot" ) ) '
        'AND PUBYEAR > 2014 AND PUBYEAR < 2027 '
        'AND ( DOCTYPE(ar) OR DOCTYPE(cp) ) '
        'AND LANGUAGE(English)'
    )

    print("\n[INFO] Avvio fetch SCOPUS (query globale)...\n")
    records = scopus.fetch_all(query)

    print(f"\n[INFO] Risultati grezzi: {len(records)}")

    # =====================================================
    # DEDUP ROBUSTO (senza eliminare record validi)
    # =====================================================

    unique = []
    seen_doi = set()
    seen_title = set()

    for r in records:
        doi = (r["doi"] or "").strip().lower()
        title = (r["title"] or "").strip().lower()

        if doi:
            if doi in seen_doi:
                continue
            seen_doi.add(doi)
        else:
            if title in seen_title:
                continue
            seen_title.add(title)

        unique.append(r)

    print(f"[INFO] Risultati unici finali: {len(unique)} (attesi: 841)")

    # =====================================================
    # SALVATAGGIO SCOPUS
    # =====================================================
    scopus.save_csv(unique, os.path.join(output_dir, "scopus_results.csv"))
    scopus.save_bib(unique, os.path.join(output_dir, "scopus_results.bib"))

    print("\n=== SCOPUS COMPLETATO ===\n")

    # =====================================================
    # ===================== GITHUB ========================
    # =====================================================
    load_dotenv("token.env")
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("⚠️ Token GitHub non trovato. Verifica token.env")

    github_fetcher = GitHubFetcher(token=github_token)

    # --- Controllo rate limit ---
    check = requests.get(
        "https://api.github.com/rate_limit",
        headers={'Authorization': f'token {github_token}'}
    )
    if check.status_code == 200:
        limits = check.json().get('resources', {}).get('search', {})
        print(f"[INFO] Limite rimanente GitHub: "
              f"{limits.get('remaining', '?')}/{limits.get('limit', '?')}")

    # --- Query GitHub ---
    keywords_cloud = ['"cloud computing"', '"cloud-computing"', '"multi-cloud"']
    keywords_ontology = [
        '"ontology"', '"ontologies"', '"semantic web"', '"knowledge graph"',
        '"knowledge graphs"', '"linked data"', '"linked open data"'
    ]
    exclusions = ['"internet of things"', 'iot']

    queries = []
    for c in keywords_cloud:
        for o in keywords_ontology:
            q = (f'{c} {o} NOT ({" OR ".join(exclusions)}) '
                 'in:name,description '
                 'created:>2014-01-01 created:<2027-01-01 '
                 'language:English')
            queries.append(q)

    all_results = []
    for q in queries:
        results = github_fetcher.fetch_repositories(query=q, max_results=200)
        all_results.extend(results)

    # Dedup GitHub
    seen = set()
    unique_results = []
    for r in all_results:
        if r['url'] not in seen:
            unique_results.append(r)
            seen.add(r['url'])

    github_fetcher.save_as_csv(unique_results, os.path.join(output_dir, "github_results.csv"))
    github_fetcher.save_as_bib(unique_results, os.path.join(output_dir, "github_results.bib"))

    # =====================================================
    # ===================== ZENODO ========================
    # =====================================================
    zenodo_fetcher = ZenodoFetcher()

    zenodo_query = (
        '("cloud computing" OR "cloud-computing" OR "multi-cloud") '
        'AND ("ontology" OR "ontologies" OR "semantic web" OR "knowledge graph" OR "knowledge graphs" '
        'OR "linked data" OR "linked open data") '
        'AND NOT ("internet of things" OR iot) '
        'AND publication_date:[2014-01-01 TO 2027-12-31]'
    )

    zenodo_records = zenodo_fetcher.fetch(zenodo_query)
    zenodo_fetcher.save_csv(zenodo_records, os.path.join(output_dir, "zenodo_results.csv"))
    zenodo_fetcher.save_bib(zenodo_records, os.path.join(output_dir, "zenodo_results.bib"))

    # =====================================================
    # ===================== LOD CLOUD =====================
    # =====================================================
    lod_fetcher = LodCloudFetcher()

    include1 = ['cloud computing', 'cloud-computing', 'multi-cloud']
    include2 = [
        'ontology', 'ontologies', 'semantic web', 'knowledge graph',
        'knowledge graphs', 'linked data', 'linked open data'
    ]
    exclude = ['internet of things', 'iot']

    lod_records = lod_fetcher.fetch([include1, include2], exclude)
    lod_fetcher.save_as_csv(lod_records, os.path.join(output_dir, "lodcloud_results.csv"))
    lod_fetcher.save_as_bib(lod_records, os.path.join(output_dir, "lodcloud_results.bib"))

    print("\n=== TUTTE LE QUERY COMPLETATE ===")


if __name__ == "__main__":
    main()
