'''import arxiv
import re
import time
from pathlib import Path
import requests
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import xmltodict

# ============================================================
# -------------------------- CONFIG ---------------------------
# ============================================================

ARXIV_QUERY = '(all:entity AND (all:resolution OR all:matching))'
PUBMED_QUERY = (
    '("ultra-processed food"[Title/Abstract] OR "ultra processed food"[Title/Abstract]) AND '
    '"cardiovascular risk"[Title/Abstract]'
)

DATASET = "pubmed"  # "arxiv" o "pubmed"

MAX_RESULTS = 5000
DELAY = 5.0
RETRY = 3
MAX_THREADS = 3

# ----------------------- CARTELLE ----------------------------
OUTPUT_DIR = Path("html_corpus")
ARXIV_DIR = OUTPUT_DIR / "arxiv_html"
PUBMED_DIR = OUTPUT_DIR / "pubmed_html"
ARXIV_DIR.mkdir(parents=True, exist_ok=True)
PUBMED_DIR.mkdir(parents=True, exist_ok=True)
LOG_ARXIV = OUTPUT_DIR / "arxiv_log.csv"
LOG_PUBMED = OUTPUT_DIR / "pubmed_log.csv"

# ----------------------- HTTP HEADERS ------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/116.0 Safari/537.36"
    )
}

# ----------------------- PATTERN -----------------------------
PATTERNS_ARXIV = [
    re.compile(r'\bentity[\s\-/]+resolution\b', re.IGNORECASE),
    re.compile(r'\bentity[\s\-/]+matching\b', re.IGNORECASE),
]

PATTERNS_PUBMED = [
    re.compile(r'ultra[\s\-]+processed\s+foods?', re.IGNORECASE),
    re.compile(r'cardiovascular\s+risk', re.IGNORECASE),
]

def matches_phrase(text: str, dataset: str) -> bool:
    if not text:
        return False
    patterns = PATTERNS_ARXIV if dataset == "arxiv" else PATTERNS_PUBMED
    return any(p.search(text) for p in patterns)


# ============================================================
# ---------------- COMMON FUNCTIONS --------------------------
# ============================================================

def load_processed(log_file):
    if not log_file.exists():
        return set()
    with open(log_file, newline="", encoding="utf-8") as f:
        return {row[0] for row in csv.reader(f)}

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
# --------------------- ARXIV FUNCTIONS ----------------------
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
# -------------------- PUBMED FUNCTIONS ----------------------
# ============================================================

def pubmed_search(query):
    """Cerca PubMed, senza filtro OA per avere più risultati."""
    url = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        f"?db=pubmed&term={query}"
        "&retmax=2000&retmode=json"
    )
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    return data["esearchresult"]["idlist"]

def get_pubmed_metadata(pmid):
    """Scarica titolo e abstract di un PMID."""
    url = (
        f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        f"?db=pubmed&id={pmid}&retmode=xml"
    )
    r = requests.get(url, headers=HEADERS)
    data = xmltodict.parse(r.text)
    try:
        article = data["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]["Article"]
        title = article.get("ArticleTitle", "").strip()
        abstract = ""
        if "Abstract" in article and "AbstractText" in article["Abstract"]:
            abs_obj = article["Abstract"]["AbstractText"]
            if isinstance(abs_obj, list):
                abstract = " ".join(
                    [a["#text"] if isinstance(a, dict) else str(a) for a in abs_obj]
                )
            else:
                abstract = abs_obj
        return title, abstract
    except:
        return "", ""

def pmid_to_pmcid(pmid):
    url = (
        f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
        f"?dbfrom=pubmed&db=pmc&id={pmid}&retmode=json"
    )
    for attempt in range(1, RETRY + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            # Controlla che sia JSON
            if "application/json" not in r.headers.get("Content-Type", ""):
                raise ValueError("Response is not JSON")
            data = r.json()
            linksets = data.get("linksets", [])
            if not linksets:
                return None
            links = linksets[0].get("linksetdb", [])
            for entry in links:
                if entry.get("dbto") == "pmc":
                    pmc_links = entry.get("links", [])
                    if pmc_links:
                        return pmc_links[0]
            return None
        except Exception as e:
            print(f"   ⚠ Error getting PMC ID for {pmid} (attempt {attempt}): {e}")
            time.sleep(DELAY)
    return None



def process_pubmed(pmid):
    title, abstract = get_pubmed_metadata(pmid)
    if matches_phrase(title, "pubmed") or matches_phrase(abstract, "pubmed"):
        print(f"[OK/PUBMED] PMID {pmid} — {title}")
        pmcid = pmid_to_pmcid(pmid)
        if pmcid:
            url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
            out_file = PUBMED_DIR / f"{pmcid}.html"
            download_html(url, out_file)
        else:
            print("   ✘ No PMC full-text")
        save_to_log(LOG_PUBMED, pmid, title)
        return pmid
    else:
        print(f"[NO/PUBMED] {pmid}")
        save_to_log(LOG_PUBMED, pmid, title)
        return None

def run_pubmed():
    print("\n=== PUBMED PROCESSING ===\n")
    pmids = pubmed_search(PUBMED_QUERY)
    pmids = pmids[:500]  # limita ai primi 500
    processed = load_processed(LOG_PUBMED)
    found = []
    tasks = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for pmid in pmids:
            if pmid in processed:
                print(f"[SKIP] {pmid}")
                continue
            tasks.append(executor.submit(process_pubmed, pmid))

        for future in as_completed(tasks):
            r = future.result()
            if r:
                found.append(r)
    print(f"\nPUBMED matched: {len(found)}")


# ============================================================
# ---------------------------- MAIN ---------------------------
# ============================================================

def main():
    if DATASET == "arxiv":
        run_arxiv()
    elif DATASET == "pubmed":
        run_pubmed()
    else:
        raise ValueError("DATASET must be 'arxiv' or 'pubmed'")

if __name__ == "__main__":
    main()'''



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
MAX_RESULTS = 5000
DELAY = 3
RETRY = 3
MAX_THREADS = 5

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
    if not log_file.exists():
        return set()
    with open(log_file, newline="", encoding="utf-8") as f:
        return {row[0] for row in csv.reader(f)}


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
    """Cerca PMC Open Access."""
    url = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        f"?db=pmc&term={query}&retmode=json&retmax={retmax}"
    )
    r = requests.get(url, headers=HEADERS)
    data = r.json()
    return data.get("esearchresult", {}).get("idlist", [])


def process_pmc(pmcid):
    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
    out_file = PMC_DIR / f"{pmcid}.html"
    downloaded = download_html(url, out_file)
    if downloaded:
        print(f"[OK/PMC] {pmcid}")
        save_to_log(LOG_PMC, pmcid, pmcid)
        return pmcid
    else:
        print(f"[FAILED/PMC] {pmcid}")
        return None


def run_pmc():
    print("\n=== PMC PROCESSING ===\n")
    pmcids = pmc_search(PMC_QUERY, retmax=1000)  # cerca fino a 1000 articoli
    pmcids = pmcids[:500]  # prendi i primi 500
    processed = load_processed(LOG_PMC)
    found = []
    tasks = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for pmcid in pmcids:
            if pmcid in processed:
                print(f"[SKIP] {pmcid}")
                continue
            tasks.append(executor.submit(process_pmc, pmcid))

        for future in as_completed(tasks):
            r = future.result()
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




