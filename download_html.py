import arxiv
import re
import time
from pathlib import Path
import requests
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import xmltodict

# ============================================================
# ---------------------- CONFIG -----------------------------
# ============================================================

DATASET = "pubmed"  # "arxiv" o "pubmed"
MAX_RESULTS = 500
DELAY = 5.0
RETRY = 3
MAX_THREADS = 3

# --------------------- QUERY -------------------------------
ARXIV_QUERY = '(all:entity AND (all:resolution OR all:matching))'

PMC_QUERY = (
    '("ultra-processed food"[Title/Abstract] OR "ultra processed food"[Title/Abstract]) '
    'AND "cardiovascular risk"[Title/Abstract]'
)

# ------------------- CARTELLE ------------------------------
OUTPUT_DIR = Path("html_corpus")
ARXIV_DIR = OUTPUT_DIR / "arxiv_html"
PMC_DIR = OUTPUT_DIR / "pmc_html"
ARXIV_DIR.mkdir(parents=True, exist_ok=True)
PMC_DIR.mkdir(parents=True, exist_ok=True)

LOG_ARXIV = OUTPUT_DIR / "arxiv_log.csv"
LOG_PMC = OUTPUT_DIR / "pmc_log.csv"

# -------------------- HEADERS -----------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/116.0 Safari/537.36"
}

# -------------------- REGEX -------------------------------
PATTERNS_ARXIV = [
    re.compile(r'\bentity[\s\-/]+resolution\b', re.IGNORECASE),
    re.compile(r'\bentity[\s\-/]+matching\b', re.IGNORECASE),
]

PATTERNS_PMC = [
    re.compile(r'ultra[\s\-]?processed\s+foods?', re.IGNORECASE),
    re.compile(r'cardiovascular\s+risk', re.IGNORECASE),
]


# ============================================================
# ---------------- COMMON FUNCTIONS -------------------------
# ============================================================

def matches_phrase(text: str, dataset: str) -> bool:
    if not text:
        return False
    patterns = PATTERNS_ARXIV if dataset == "arxiv" else PATTERNS_PMC
    return any(p.search(text) for p in patterns)


def load_processed(log_file):
    """Carica gli ID già scaricati evitando errori su righe vuote o corrotte."""
    if not log_file.exists():
        return set()

    processed = set()
    with open(log_file, newline='', encoding='utf-8') as f:
        for row in csv.reader(f):
            if row and row[0].strip():   # evita linee vuote o corrotte
                processed.add(row[0].strip())
    return processed


def save_to_log(log_file, identifier: str, title: str):
    with open(log_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([identifier, title])


def download_html(url: str, out_file: Path):
    if out_file.exists():
        return out_file
    for attempt in range(1, RETRY + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            out_file.write_text(r.text, encoding="utf-8")
            return out_file
        except Exception as e:
            print(f"   ⚠ Error downloading {url} (attempt {attempt}): {e}")
            time.sleep(DELAY)
    return None


# ============================================================
# --------------------- ARXIV ------------------------------
# ============================================================

def process_arxiv(result):
    arxiv_id = result.get_short_id()
    title = (result.title or "").strip()
    summary = (result.summary or "").strip()

    if matches_phrase(title, "arxiv") or matches_phrase(summary, "arxiv"):
        print(f"[OK/ARXIV] {arxiv_id} — {title}")
        url = f"https://arxiv.org/html/{arxiv_id}"
        out_file = ARXIV_DIR / f"{arxiv_id}.html"
        download_html(url, out_file)
        save_to_log(LOG_ARXIV, arxiv_id, title)
        return arxiv_id
    else:
        print(f"[NO/ARXIV] {arxiv_id}")
        save_to_log(LOG_ARXIV, arxiv_id, title)
        return None


def run_arxiv():
    print("\n=== ARXIV PROCESSING ===\n")
    client = arxiv.Client()
    search = arxiv.Search(
        query=ARXIV_QUERY,
        max_results=MAX_RESULTS,
        sort_by=arxiv.SortCriterion.Relevance
    )
    processed = load_processed(LOG_ARXIV)
    found = []
    tasks = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for res in client.results(search):
            arxiv_id = res.get_short_id()
            if arxiv_id in processed:
                print(f"[SKIP] {arxiv_id}")
                continue
            tasks.append(executor.submit(process_arxiv, res))

        for future in as_completed(tasks):
            r = future.result()
            if r:
                found.append(r)
    print(f"\nARXIV matched: {len(found)}")


# ============================================================
# --------------------- PMC -------------------------------
# ============================================================

def pmc_search(query, retmax=500):
    """Cerca PMC Open Access e restituisce gli ID."""
    url = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        f"?db=pmc&term={query}&retmode=json&retmax={retmax}"
    )
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    return data.get("esearchresult", {}).get("idlist", [])


def process_pmc(pmcid):
    """Scarica HTML di un articolo PMC tramite efetch per evitare 403."""
    out_file = PMC_DIR / f"{pmcid}.html"

    if out_file.exists():
        print(f"[SKIP/PMC] {pmcid} (already downloaded)")
        return pmcid

    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={pmcid}&retmode=html"

    for attempt in range(1, RETRY + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 404:
                print(f"[404/PMC] {pmcid}")
                return None
            r.raise_for_status()
            out_file.write_text(r.text, encoding="utf-8")
            print(f"[OK/PMC] {pmcid}")
            save_to_log(LOG_PMC, pmcid, pmcid)
            time.sleep(DELAY)  # piccolo delay tra le richieste
            return pmcid
        except requests.HTTPError as e:
            print(f"   ⚠ HTTP Error {pmcid} (attempt {attempt}): {e}")
            time.sleep(DELAY)
        except requests.RequestException as e:
            print(f"   ⚠ Request error {pmcid} (attempt {attempt}): {e}")
            time.sleep(DELAY)

    print(f"[FAILED/PMC] {pmcid}")
    return None


def run_pmc():
    print("\n=== PMC PROCESSING ===\n")
    pmcids = pmc_search(PMC_QUERY, retmax=1000)[:500]
    processed = load_processed(LOG_PMC)
    found = []

    # Scarica sequenzialmente per evitare 403
    for pmcid in pmcids:
        if pmcid in processed:
            print(f"[SKIP] {pmcid}")
            continue
        r = process_pmc(pmcid)
        if r:
            found.append(r)

    print(f"\nPMC matched: {len(found)}")



# ============================================================
# ------------------------ MAIN -----------------------------
# ============================================================

def main():
    if DATASET == "arxiv":
        run_arxiv()
    elif DATASET == "pubmed":
        run_pmc()
    else:
        raise ValueError("DATASET must be 'arxiv' or 'pubmed'")


if __name__ == "__main__":
    main()