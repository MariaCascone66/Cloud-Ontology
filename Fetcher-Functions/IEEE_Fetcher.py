import time
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

class IEEEScraper:
    def __init__(self, headless=True):
        options = Options()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        self.driver = webdriver.Chrome(options=options)
        self.base_url = "https://ieeexplore.ieee.org/search/searchresult.jsp"
        self.results = []

        # Query originale
        self.query = (
            '("cloud computing" OR "cloud-computing" OR "multi-cloud") AND '
            '("ontolog*" OR "semantic web" OR "knowledge graph*" OR "linked data" OR "linked open data") '
            'AND NOT ("internet of things" OR "iot")'
        )
        self.start_year = 2015
        self.end_year = 2025

    def scrape(self, max_pages=50, pause=3):
        page_num = 1
        while True:
            print(f"[INFO] Scaricando pagina {page_num}...")
            search_url = (
                f'{self.base_url}?queryText={self.query}&'
                f'ranges={self.start_year}_{self.end_year}_Year&'
                f'pageNumber={page_num}'
            )
            self.driver.get(search_url)
            time.sleep(pause)

            # Controlla se ci sono risultati
            articles = self.driver.find_elements(By.CSS_SELECTOR, "div.List-results-items > div.List-results-item")
            if not articles:
                print(f"[INFO] Nessun articolo trovato a pagina {page_num}. Fine scraping.")
                break

            for art in articles:
                try:
                    title_elem = art.find_element(By.CSS_SELECTOR, "h2 a")
                    title = title_elem.text
                    url = title_elem.get_attribute("href")
                    authors_elem = art.find_elements(By.CSS_SELECTOR, "p.author span")
                    authors = ", ".join([a.text for a in authors_elem])
                    journal_elem = art.find_element(By.CSS_SELECTOR, "span.publisher-info-container")
                    journal = journal_elem.text
                    year_elem = art.find_element(By.CSS_SELECTOR, "div.description > span:nth-child(2)")
                    year = year_elem.text.split()[-1]  # ultima parola
                    self.results.append({
                        "title": title,
                        "authors": authors,
                        "journal": journal,
                        "year": year,
                        "url": url
                    })
                except Exception as e:
                    continue  # salta se qualche elemento manca

            page_num += 1
            if page_num > max_pages:
                break

        print(f"[INFO] Totale articoli raccolti: {len(self.results)}")
        self.driver.quit()
        return self.results

    def save_csv(self, filename="ieee_results.csv"):
        if not self.results:
            print("[WARN] Nessun dato da salvare.")
            return
        keys = self.results[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(self.results)
        print(f"[INFO] File CSV salvato come '{filename}'")

    def save_bib(self, filename="ieee_results.bib"):
        if not self.results:
            print("[WARN] Nessun dato da salvare.")
            return
        with open(filename, 'w', encoding='utf-8') as f:
            for i, rec in enumerate(self.results):
                key = f"ieee{i+1}"
                title = rec["title"].replace("{", "\\{").replace("}", "\\}")
                authors = rec["authors"].replace("{", "\\{").replace("}", "\\}")
                year = rec["year"]
                url = rec["url"]
                journal = rec["journal"]
                f.write(f"@article{{{key},\n"
                        f"  title={{ {title} }},\n"
                        f"  author={{ {authors} }},\n"
                        f"  journal={{ {journal} }},\n"
                        f"  year={{ {year} }},\n"
                        f"  url={{ {url} }}\n}}\n\n")
        print(f"[INFO] File BibTeX salvato come '{filename}'")


if __name__ == "__main__":
    scraper = IEEEScraper()
    results = scraper.scrape(max_pages=30)  # imposta numero massimo di pagine
    scraper.save_csv(r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\ieee_results.csv")
    scraper.save_bib(r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\ieee_results.bib")
