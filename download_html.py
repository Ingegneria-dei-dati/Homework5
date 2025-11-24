import re
import time
import csv
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

# --- CONFIG ---
DATASET = "pubmed"  # "arxiv" o "pubmed"
MAX_RESULTS = 500
DELAY = 1.0        # delay tra retry
RETRY = 3          # retry per download falliti
MAX_THREADS = 5    # thread simultanei

OUTPUT_DIR = Path("html_corpus")
HTML_DIR = OUTPUT_DIR / "html"
HTML_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUTPUT_DIR / "downloaded_log.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/116.0 Safari/537.36"
    )
}

# --- QUERY e PATTERNS ---
if DATASET == "arxiv":
    QUERY = '(all:entity AND (all:resolution OR all:matching))'
    PATTERNS = [
        re.compile(r'\bentity[\s\-/]+resolution\b', re.IGNORECASE),
        re.compile(r'\bentity[\s\-/]+matching\b', re.IGNORECASE),
    ]
elif DATASET == "pubmed":
    QUERY = "ultra-processed foods AND cardiovascular risk"
    PATTERNS = [
        re.compile(r'\bultra[\s\-/]+processed[\s\-/]+foods\b', re.IGNORECASE),
        re.compile(r'\bcardiovascular[\s\-/]+risk\b', re.IGNORECASE),
    ]
else:
    raise ValueError("Dataset non supportato: scegli 'arxiv' o 'pubmed'")

# --- FUNZIONI UTILI ---
def matches_phrase(text: str) -> bool:
    if not text:
        return False
    return any(p.search(text) for p in PATTERNS)

def load_processed():
    if not LOG_FILE.exists():
        return set()
    with open(LOG_FILE, newline="", encoding="utf-8") as f:
        return {row[0] for row in csv.reader(f)}

def save_to_log(item_id: str, title: str):
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([item_id, title])

def download_html(url: str, out_file: Path):
    if out_file.exists():
        return out_file
    for attempt in range(1, RETRY+1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30, stream=True)
            r.raise_for_status()
            content = b"".join(r.iter_content(8192))
            out_file.write_bytes(content)
            return out_file
        except requests.RequestException as e:
            print(f"   ⚠ Errore download {out_file.name} (tentativo {attempt}): {e}")
            time.sleep(DELAY)
    return None

# --- PROCESS PAPER ---
def process_arxiv_paper(result):
    arxiv_id = result.get_short_id()
    title = (result.title or "").strip()
    summary = (result.summary or "").strip()

    if matches_phrase(title) or matches_phrase(summary):
        print(f"[OK] {arxiv_id} — {title}")
        html_path = download_html(f"https://arxiv.org/html/{arxiv_id}", HTML_DIR / f"{arxiv_id}.html")
        if html_path:
            print(f"   ✔ HTML salvato: {html_path.name}")
        save_to_log(arxiv_id, title)
        return arxiv_id
    else:
        print(f"[NO] {arxiv_id}")
        save_to_log(arxiv_id, title)
        return None

def process_pubmed_id(pubmed_id, processed):
    if pubmed_id in processed:
        print(f"[SKIP] {pubmed_id}")
        return None

    # --- ESummary ---
    r2 = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
        params={"db": "pubmed", "id": pubmed_id, "retmode": "json"},
        headers=HEADERS
    )
    r2.raise_for_status()
    doc = r2.json()["result"].get(pubmed_id, {})
    title = doc.get("title", "")

    # --- ELink ---
    r3 = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi",
        params={"dbfrom": "pubmed", "db": "pmc", "id": pubmed_id},
        headers=HEADERS
    )
    r3.raise_for_status()
    pmcid = None
    xml_root = ET.fromstring(r3.text)
    for linksetdb in xml_root.iter("LinkSetDb"):
        if linksetdb.find("LinkName").text == "pubmed_pmc":
            pmcid = "PMC" + linksetdb.find("Link/Id").text
            break

    if not pmcid:
        print(f"[NO PMC] {pubmed_id}")
        save_to_log(pubmed_id, title)
        return None

    if matches_phrase(title):
        print(f"[OK] {pubmed_id} / {pmcid}")
        html_path = download_html(f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/",
                                  HTML_DIR / f"{pmcid}.html")
        if html_path:
            print(f"   ✔ HTML salvato")
            save_to_log(pubmed_id, title)
            return pmcid
        else:
            print(f"   ✘ HTML non scaricato")
    else:
        print(f"[NO MATCH] {pubmed_id}")
        save_to_log(pubmed_id, title)
    return None

# --- MAIN ---
def main():
    processed = load_processed()
    found = []

    if DATASET == "arxiv":
        import arxiv
        client = arxiv.Client()
        search = arxiv.Search(query=QUERY, max_results=MAX_RESULTS,
                              sort_by=arxiv.SortCriterion.Relevance)
        tasks = []
        print("Processo Arxiv multithread...")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for result in client.results(search):
                tasks.append(executor.submit(process_arxiv_paper, result))
            for future in as_completed(tasks):
                res = future.result()
                if res:
                    found.append(res)

    elif DATASET == "pubmed":
        # --- ESEARCH ---
        r = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params={"db": "pubmed", "term": QUERY, "retmax": MAX_RESULTS, "retmode": "json"},
            headers=HEADERS
        )
        r.raise_for_status()
        id_list = r.json()["esearchresult"]["idlist"]

        print("Processo PubMed multithread...")
        tasks = []
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for pid in id_list:
                tasks.append(executor.submit(process_pubmed_id, pid, processed))
            for future in as_completed(tasks):
                res = future.result()
                if res:
                    found.append(res)

    print("\n--- RISULTATI FINALI ---")
    print(f"Articoli trovati: {len(found)}")
    for x in found:
        print(f" - {x}")

    return found

if __name__ == "__main__":
    main()
