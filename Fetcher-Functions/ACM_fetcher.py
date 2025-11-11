import requests
from bs4 import BeautifulSoup
import csv
import time

class ACMScraper:
    def __init__(self):
        self.base_url = "https://dl.acm.org/action/doSearch"
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

    def build_query_params(self, page=1):
        # 🔹 Query ACM DL esatta
        query = (
            '(Title:("cloud computing" OR "cloud-computing" OR "multi-cloud") '
            'OR Abstract:("cloud computing" OR "cloud-computing" OR "multi-cloud") '
            'OR Keyword:("cloud computing" OR "cloud-computing" OR "multi-cloud")) '
            'AND '
            '(Title:("ontolog*" OR "semantic web" OR "knowledge graph*" OR "linked data" OR "linked open data") '
            'OR Abstract:("ontolog*" OR "semantic web" OR "knowledge graph*" OR "linked data" OR "linked open data") '
            'OR Keyword:("ontolog*" OR "semantic web" OR "knowledge graph*" OR "linked data" OR "linked open data")) '
            'AND !(Title:("internet of things" OR "iot") '
            'OR Abstract:("internet of things" OR "iot") '
            'OR Keyword:("internet of things" OR "iot"))'
        )
        return {
            "AllField": query,
            "startPage": page,
            "pageSize": 20  # numero di risultati per pagina
        }

    def fetch_page(self, page=1):
        params = self.build_query_params(page)
        response = self.session.get(self.base_url, headers=self.headers, params=params)
        if response.status_code != 200:
            print(f"[ERROR] Pagina {page} non disponibile: {response.status_code}")
            return []
        soup = BeautifulSoup(response.text, "html.parser")
        results = []

        # 🔹 Trova articoli nella pagina
        articles = soup.select("div.search__item")  # selettore CSS ACM DL
        for art in articles:
            title_tag = art.select_one("h5.issue-item__title a")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)
            url = "https://dl.acm.org" + title_tag.get("href")
            authors = ", ".join([a.get_text(strip=True) for a in art.select("ul.rlist--inline li span.hlFld-ContribAuthor")])
            abstract_tag = art.select_one("div.issue-item__abstract")
            abstract = abstract_tag.get_text(strip=True) if abstract_tag else ""
            keywords_tag = art.select("div.issue-item__keywords span")
            keywords = ", ".join([k.get_text(strip=True) for k in keywords_tag]) if keywords_tag else ""

            results.append({
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "keywords": keywords,
                "url": url
            })

        return results

    def fetch_all(self, max_pages=5):
        all_results = []
        for page in range(1, max_pages + 1):
            print(f"[INFO] Scaricando pagina {page}...")
            page_results = self.fetch_page(page)
            if not page_results:
                break
            all_results.extend(page_results)
            time.sleep(2)  # pausa per rispettare ToS
        print(f"[INFO] Totale articoli raccolti: {len(all_results)}")
        return all_results

    def save_as_csv(self, data, filename="acm_scraped.csv"):
        if not data:
            print("[WARN] Nessun dato da salvare.")
            return
        fieldnames = ["title", "authors", "abstract", "keywords", "url"]
        with open(filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"[INFO] File CSV salvato come {filename}")


if __name__ == "__main__":
    scraper = ACMScraper()
    articles = scraper.fetch_all(max_pages=2)  # dovrebbe bastare per i 19 risultati
    scraper.save_as_csv(articles, r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\acm_scraped.csv")
