import re
import time
import csv
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# --- CONFIG ---
QUERY = "ultra-processed foods AND cardiovascular risk"
MAX_RESULTS = 500
DELAY = 1.0       # delay base tra richieste
RETRY = 3         # retry per download falliti
MAX_THREADS = 3   # numero di thread simultanei

OUTPUT_DIR = Path("pubmed_html_corpus")
HTML_DIR = OUTPUT_DIR / "html"
HTML_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUTPUT_DIR / "downloaded_log.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://google.com"
}

PATTERNS = [
    re.compile(r"ultra[\s\-/]+processed", re.IGNORECASE),
    re.compile(r"cardiovascular[\s\-/]+risk", re.IGNORECASE)
]

# --- FUNZIONI ---
def matches_phrase(text: str) -> bool:
    if not text:
        return False
    return all(p.search(text) for p in PATTERNS)

def load_processed():
    if not LOG_FILE.exists():
        return set()
    with open(LOG_FILE, newline="", encoding="utf-8") as f:
        return {row[0] for row in csv.reader(f)}

def save_to_log(pubmed_id, pmcid, title):
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([pubmed_id, pmcid, title])

def safe_json(resp):
    text = resp.content.decode("utf-8", errors="replace")
    return json.loads(text)

def download_html(pmcid: str):
    out_file = HTML_DIR / f"{pmcid}.html"
    if out_file.exists():
        return out_file

    urls = [
        f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/",
        f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/",
    ]

    for url in urls:
        for attempt in range(RETRY):
            try:
                r = requests.get(url, headers=HEADERS, timeout=30)
                if r.status_code == 403:
                    raise Exception("Access denied (403)")
                r.raise_for_status()
                if "<title>Access denied" in r.text or "Robot" in r.text:
                    raise Exception("Blocked by anti-bot system")
                out_file.write_text(r.text, encoding="utf-8")
                return out_file
            except Exception as e:
                wait = 2 ** attempt
                print(f"   ⚠ Tentativo {attempt+1}/{RETRY} fallito ({pmcid}) — {e}, retry in {wait}s")
                time.sleep(wait)
    print(f"   ✘ Impossibile scaricare {pmcid}")
    return None

def process_article(pubmed_id: str):
    # --- ESUMMARY ---
    try:
        r_sum = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            params={"db": "pubmed", "id": pubmed_id, "retmode": "json"},
            headers=HEADERS
        )
        r_sum.raise_for_status()
        doc = safe_json(r_sum)["result"].get(pubmed_id, {})
        title = doc.get("title", "").strip()
    except:
        title = ""

    # --- EFETCH per abstract ---
    try:
        r_abs = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
            params={"db": "pubmed", "id": pubmed_id, "rettype": "abstract", "retmode": "text"},
            headers=HEADERS
        )
        abstract = r_abs.text
    except:
        abstract = ""

    # --- ELink per PMC ---
    try:
        r_link = requests.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi",
            params={"dbfrom": "pubmed", "db": "pmc", "id": pubmed_id, "retmode": "json"},
            headers=HEADERS
        )
        r_link.raise_for_status()
        pmcid = None
        linkdbs = safe_json(r_link)["linksets"][0].get("linksetdbs", [])
        for db in linkdbs:
            if db.get("linkname") == "pubmed_pmc":
                pmcid = db["links"][0]
                break
    except:
        pmcid = None

    if not pmcid:
        print(f"[NO PMC] {pubmed_id} — no open access")
        save_to_log(pubmed_id, "", title)
        return None

    # --- MATCH ---
    if matches_phrase(title) or matches_phrase(abstract):
        print(f"[OK] {pubmed_id} → {pmcid} — {title}")
        html_path = download_html(pmcid)
        if html_path:
            print(f"   ✔ HTML salvato: {html_path.name}")
            save_to_log(pubmed_id, pmcid, title)
            return pmcid
        else:
            print(f"   ✘ HTML non scaricato")
    else:
        print(f"[NO MATCH] {pubmed_id}")
        save_to_log(pubmed_id, pmcid, title)
    time.sleep(DELAY)
    return None

# --- MAIN ---
def main():
    processed = load_processed()
    found = []

    # --- ESEARCH ---
    r = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
        params={"db": "pubmed", "term": QUERY, "retmax": MAX_RESULTS, "retmode": "json"},
        headers=HEADERS
    )
    r.raise_for_status()
    id_list = r.json()["esearchresult"]["idlist"]
    print(f"Trovati {len(id_list)} articoli.\n")

    tasks = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for pubmed_id in id_list:
            if pubmed_id in processed:
                print(f"[SKIP] {pubmed_id} già processato")
                continue
            tasks.append(executor.submit(process_article, pubmed_id))

        for future in as_completed(tasks):
            res = future.result()
            if res:
                found.append(res)

    print("\n--- RISULTATI FINALI ---")
    print(f"Articoli processati: {len(id_list)}")
    print(f"HTML open access scaricati con entrambe le frasi: {len(found)}")
    for x in found:
        print(" -", x)

if __name__ == "__main__":
    main()
