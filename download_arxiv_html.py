'''import arxiv
import re
import time
from pathlib import Path

# Query permissiva ma ragionevole
QUERY = '(all:entity AND (all:resolution OR all:matching))'

DELAY = 1.0
MAX_RESULTS = 5000

# pattern per cercare solo le frasi esatte (con eventuali - o /)
PATTERNS = [
    re.compile(r'\bentity[\s\-/]+resolution\b', re.IGNORECASE),
    re.compile(r'\bentity[\s\-/]+matching\b', re.IGNORECASE),
]

def matches_phrase(text: str) -> bool:
    """Ritorna True se una frase esatta è trovata nel testo."""
    if not text:
        return False
    for p in PATTERNS:
        if p.search(text):
            return True
    return False

def main():
    client = arxiv.Client()
    search = arxiv.Search(
        query=QUERY,
        max_results=MAX_RESULTS,
        sort_by=arxiv.SortCriterion.Relevance
    )

    found = []
    examined = 0

    print("Filtro locale su title/abstract per frasi esatte…\n")

    for result in client.results(search):
        examined += 1

        arxiv_id = result.get_short_id()
        title = (result.title or "").strip()
        summary = (result.summary or "").strip()

        # titolo o abstract contengono la frase esatta?
        if matches_phrase(title) or matches_phrase(summary):
            print(f"[OK] {arxiv_id}  — {title}")
            found.append((arxiv_id, title))
        else:
            print(f"[NO] {arxiv_id}")

        time.sleep(DELAY)

    print("\n--- RISULTATI FINALI ---")
    print(f"Articoli esaminati: {examined}")
    print(f"Articoli che contengono la frase esatta: {len(found)}")
    for aid, t in found:
        print(f" - {aid}: {t}")

    return found

if __name__ == "__main__":
    main()
#stampa: 
#Articoli esaminati: 1506
#Articoli che contengono la frase esatta: 309
'''








'''import arxiv
import re
import time
from pathlib import Path
import requests

# --- QUERY ---
QUERY = '(all:entity AND (all:resolution OR all:matching))'
MAX_RESULTS = 5000

# --- DELAY PER EVITARE BLOCCO IP ---
DELAY = 5.0  # aumenta a 8–10 se scarichi molti file

# --- CARTELLE ---
OUTPUT_DIR = Path("arxiv_html_corpus")
HTML_DIR = OUTPUT_DIR / "html"
HTML_DIR.mkdir(parents=True, exist_ok=True)

# --- HEADER PER EVITARE 403 ---
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/116.0 Safari/537.36"
    )
}

# --- PATTERN DELLE FRASI ESATTE ---
PATTERNS = [
    re.compile(r'\bentity[\s\-/]+resolution\b', re.IGNORECASE),
    re.compile(r'\bentity[\s\-/]+matching\b', re.IGNORECASE),
]

def matches_phrase(text: str) -> bool:
    if not text:
        return False
    return any(p.search(text) for p in PATTERNS)

# --- DOWNLOAD HTML SICURO ---
def download_html(arxiv_id: str):
    url = f"https://arxiv.org/html/{arxiv_id}"
    out_file = HTML_DIR / f"{arxiv_id}.html"

    # non riscaricare file già ottenuto
    if out_file.exists():
        return out_file

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code == 404:
            print(f"   ⚠ HTML non disponibile (404)")
            return None

        r.raise_for_status()
        out_file.write_text(r.text, encoding="utf-8")
        return out_file

    except Exception as e:
        print(f"   ⚠ Errore download: {e}")
        return None

    finally:
        time.sleep(DELAY)


# --- MAIN ---
def main():
    client = arxiv.Client()
    search = arxiv.Search(
        query=QUERY,
        max_results=MAX_RESULTS,
        sort_by=arxiv.SortCriterion.Relevance
    )

    found = []
    examined = 0

    print("Filtro su title/abstract + download HTML…\n")

    for result in client.results(search):
        examined += 1
        arxiv_id = result.get_short_id()
        title = (result.title or "").strip()
        summary = (result.summary or "").strip()

        # match su titolo o abstract
        if matches_phrase(title) or matches_phrase(summary):
            print(f"[OK] {arxiv_id} — {title}")
            found.append(arxiv_id)

            html_path = download_html(arxiv_id)
            if html_path:
                print(f"   ✔ HTML salvato: {html_path.name}")
            else:
                print("   ✘ HTML non scaricato")

        else:
            print(f"[NO] {arxiv_id}")

        time.sleep(DELAY)

    print("\n--- RISULTATI FINALI ---")
    print(f"Articoli esaminati: {examined}")
    print(f"Articoli che contengono la frase esatta: {len(found)}")
    for aid in found:
        print(f" - {aid}")

    return found


if __name__ == "__main__":
    main()
    #si blocca dopo un po' '''


import arxiv
import re
import time
from pathlib import Path
import requests
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIG ---
QUERY = '(all:entity AND (all:resolution OR all:matching))'
MAX_RESULTS = 5000
DELAY = 5.0        # delay tra retry
RETRY = 3          # retry per download falliti
MAX_THREADS = 3    # numero di thread simultanei

# --- CARTELLE ---
OUTPUT_DIR = Path("arxiv_html_corpus")
HTML_DIR = OUTPUT_DIR / "html"
HTML_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = OUTPUT_DIR / "downloaded_log.csv"

# --- HEADER ---
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/116.0 Safari/537.36"
    )
}

# --- PATTERN ---
PATTERNS = [
    re.compile(r'\bentity[\s\-/]+resolution\b', re.IGNORECASE),
    re.compile(r'\bentity[\s\-/]+matching\b', re.IGNORECASE),
]

def matches_phrase(text: str) -> bool:
    if not text:
        return False
    return any(p.search(text) for p in PATTERNS)

def download_html(arxiv_id: str):
    """Scarica HTML con retry e delay."""
    out_file = HTML_DIR / f"{arxiv_id}.html"
    if out_file.exists():
        return out_file

    url = f"https://arxiv.org/html/{arxiv_id}"
    for attempt in range(1, RETRY + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 404:
                print(f"   ⚠ HTML non disponibile (404) per {arxiv_id}")
                return None
            r.raise_for_status()
            out_file.write_text(r.text, encoding="utf-8")
            return out_file
        except Exception as e:
            print(f"   ⚠ Errore {arxiv_id} (tentativo {attempt}): {e}")
            time.sleep(DELAY)
    return None

def load_processed():
    processed = set()
    if LOG_FILE.exists():
        with open(LOG_FILE, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            processed = {row[0] for row in reader}
    return processed

def save_to_log(arxiv_id: str, title: str):
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([arxiv_id, title])

def process_paper(result):
    """Filtra e scarica HTML di un singolo paper."""
    arxiv_id = result.get_short_id()
    title = (result.title or "").strip()
    summary = (result.summary or "").strip()

    if matches_phrase(title) or matches_phrase(summary):
        print(f"[OK] {arxiv_id} — {title}")
        html_path = download_html(arxiv_id)
        if html_path:
            print(f"   ✔ HTML salvato: {html_path.name}")
        else:
            print(f"   ✘ HTML non scaricato")
        save_to_log(arxiv_id, title)
        return arxiv_id
    else:
        print(f"[NO] {arxiv_id}")
        save_to_log(arxiv_id, title)
        return None

def main():
    client = arxiv.Client()
    search = arxiv.Search(
        query=QUERY,
        max_results=MAX_RESULTS,
        sort_by=arxiv.SortCriterion.Relevance
    )

    processed = load_processed()
    found = []
    examined = 0
    tasks = []

    print("Filtro su title/abstract + download HTML multithread…\n")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for result in client.results(search):
            examined += 1
            arxiv_id = result.get_short_id()

            if arxiv_id in processed:
                print(f"[SKIP] {arxiv_id} già processato")
                continue

            # invia al thread pool solo i paper da processare
            tasks.append(executor.submit(process_paper, result))

        # raccogli risultati
        for future in as_completed(tasks):
            res = future.result()
            if res:
                found.append(res)

    print("\n--- RISULTATI FINALI ---")
    print(f"Articoli esaminati: {examined}")
    print(f"Articoli che contengono la frase esatta: {len(found)}")
    for aid in found:
        print(f" - {aid}")

    return found

if __name__ == "__main__":
    main()
