import os
import glob
import hashlib
import re
import datetime
from bs4 import BeautifulSoup, element
from elasticsearch import Elasticsearch

# ============================================================
# 1. CONNESSIONE ELASTICSEARCH
# ============================================================

ES = Elasticsearch("http://localhost:9200")
INDEX_NAME = "research_articles_v2"

# ============================================================
# 2. MAPPING CON ANALYZER PERSONALIZZATI
# ============================================================

MAPPING = {
    "settings": {
        "analysis": {
            "analyzer": {
                "english_custom": {
                    "type": "english",
                    "stopwords": "_english_"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "title": {"type": "text", "analyzer": "english_custom"},
            "authors": {"type": "text", "analyzer": "standard"},
            "date": {
                "type": "date",
                "format": "yyyy-MM-dd||yyyy||dd MMM yyyy",
                "null_value": None
            },
            "abstract": {"type": "text", "analyzer": "english_custom"},
            "paragraphs": {"type": "text", "analyzer": "english_custom"},
            "content_full": {"type": "text", "analyzer": "english_custom"},
            "source": {"type": "keyword"},
            "file_path": {"type": "keyword"}
        }
    }
}

# ============================================================
# 3. CREA INDICE SE NON ESISTE
# ============================================================

def create_index():
    if ES.indices.exists(index=INDEX_NAME):
        print(f"Indice '{INDEX_NAME}' già esistente")
    else:
        try:
            ES.indices.create(index=INDEX_NAME, body=MAPPING)
            print(f"Indice '{INDEX_NAME}' creato.")
        except Exception as e:
            print(f"ERRORE: Impossibile creare l'indice: {e}")

# ============================================================
# 4. ESTRAZIONE METADATI E PARAGRAFI DA HTML (Pulizia Autori Finale)
# ============================================================

def parse_html(filepath: str):
    """Estrae i metadati e il testo completo (suddiviso in paragrafi) dal file HTML."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
    except Exception as e:
        print(f"Errore nella lettura del file {filepath}: {e}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    title, abstract, date = "", "", None
    authors = []

    # --- A. PARSING METADATI (Priorità Alta) ---

    title_tag = soup.find("meta", {"name": "citation_title"}) or soup.find("meta", {"name": "DC.title"}) or soup.title
    if title_tag:
        title = title_tag.get("content") if title_tag.name == 'meta' else title_tag.get_text(strip=True)
    if not title:
        h1 = soup.find("h1")
        if h1: title = h1.get_text(strip=True)

    for tag in soup.find_all("meta", {"name": ["citation_author", "DC.creator", "author"]}):
        author_content = tag.get("content")
        if author_content and author_content not in authors:
            authors.append(author_content)

    date_tag = soup.find("meta", {"name": "citation_publication_date"}) or soup.find("meta", {"name": "citation_date"})
    if date_tag:
        date = date_tag.get("content")

    abs_meta = soup.find("meta", {"name": "description"}) or soup.find("meta", {"name": "abstract"})
    if abs_meta:
        abstract = abs_meta.get("content").strip()

    # --- B. PARSING BLOCCHI SPECIFICI (Fallback Arxiv) ---

    if not authors:
        author_div = soup.find("div", {"class": "authors"})
        if author_div:
            author_links = author_div.find_all("a")
            authors.extend([link.get_text(strip=True) for link in author_links if link.get_text(strip=True) and link.get_text(strip=True) not in authors])
            if not authors:
                authors_text = author_div.get_text(separator=",", strip=True)
                authors.extend([a.strip() for a in authors_text.split(",") if a.strip()])

    if not abstract:
        abs_tag = soup.find("blockquote", {"id": "abstract"}) or soup.find("div", {"class": "abstract"})
        if abs_tag:
            abstract_text = abs_tag.get_text(strip=True)
            abstract = re.sub(r'^(Abstract:?\s*|abstract:?\s*)', '', abstract_text, flags=re.IGNORECASE).strip()

    if not date:
        date_tag = soup.find("span", {"class": "date"}) or soup.find("div", {"class": "date"})
        if date_tag:
            date = date_tag.get_text(strip=True)

    # -----------------------------------------------------------------
    # C. EURISTICA DEL CORPO (Pulizia e Gestione Data)
    # -----------------------------------------------------------------

    content_root = soup.find('div', id='content') or soup.find('article') or soup.body
    if content_root:
        full_text_start = content_root.get_text(" ", strip=True)[:3000]
        text_for_author_search = full_text_start

        # Pulizia del testo di partenza per isolare gli Autori
        if title:
            text_for_author_search = re.sub(re.escape(title), '', text_for_author_search, flags=re.IGNORECASE)
        if abstract:
            text_for_author_search = re.sub(re.escape(abstract), '', text_for_author_search, flags=re.IGNORECASE)
        text_for_author_search = re.sub(r'^(abstract|summary|introduction)\s*[:.]?', '', text_for_author_search, flags=re.IGNORECASE).strip()

        # 1. Fallback Autori (Euristica sul testo PULITO - Con lista minimalista)
        if not authors:
            author_candidates = re.findall(r'([A-Z][a-z]+\s+(?:[A-Z]\.\s*)?[A-Z][a-z]+)', text_for_author_search[:500])

            # Lista di parole chiave di affiliazione/località da escludere
            INSTITUTION_KEYWORDS = ['University', 'Institute', 'Department', 'Science', 'Group', 'College',
                                    'Amazon', 'Seattle', 'Mannheim', 'Ohio', 'Web', 'State']

            CLEAN_AUTHORS_CANDIDATES = []
            for a in author_candidates:
                a_clean = a.strip()

                if re.search(r'^(Abstract|Introduction|Figure|Table|Appendix|Pipelines|Documents|Group)', a_clean, re.IGNORECASE):
                    continue

                if len(a_clean.split()) > 4:
                    continue

                if re.search(r'\d+', a_clean):
                    continue

                # Regola CRITICA: Esclusione per parole chiave istituzionali (più efficace)
                if any(kw.lower() in a_clean.lower() for kw in INSTITUTION_KEYWORDS):
                    continue

                # Rimuovi ID/suffissi in minuscolo o singole lettere finali (es. 'alexander', 'r')
                a_final = re.sub(r'\s+[a-z]+$', '', a_clean)
                a_final = re.sub(r'\s+\w$', '', a_final)

                if a_final and len(a_final.split()) >= 2:
                    CLEAN_AUTHORS_CANDIDATES.append(a_final)

            authors.extend(list(dict.fromkeys(CLEAN_AUTHORS_CANDIDATES))[:15])

        # 2. Fallback Abstract
        if not abstract:
            abstract_match = re.search(r'(abstract|summary)\s*[:.]?\s*(.*?)(?=\s*(?:[I.]{2,}\s|\n\n|\n[A-Z]|$|References))',
                                       full_text_start, re.IGNORECASE | re.DOTALL)
            if abstract_match:
                abstract = abstract_match.group(2).strip()

        # 3. Fallback Data (Ricerca e Normalizzazione)
        if not date:
            date_match_full = re.search(
                r'(?i)(?:submitted\s+on|version\s+of|v\d+\])\s*(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
                full_text_start[:1000]
            )

            if date_match_full:
                date_str = date_match_full.group(1).strip()
                try:
                    date_obj = datetime.datetime.strptime(date_str, '%d %b %Y')
                    date = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    date = date_str
            else:
                date_match_year = re.search(r'(\d{4})', full_text_start[:500])
                if date_match_year:
                    year = date_match_year.group(1)
                    if int(year) > 2000 and int(year) <= datetime.date.today().year:
                        date = year

                        # CRITICAL FIX: Imposta 'date' a None se non è una stringa valida
    if date is not None and (isinstance(date, str) and (not date.strip() or '0000' in date)):
        date = None

    # RIMUOVE I DUPLICATI FINALI DAGLI AUTORI
    authors_str = ", ".join(list(dict.fromkeys(authors)))

    # -----------------------------------------------------------------
    # D. ESTRAZIONE TESTO COMPLETO E PARAGRAFI (Pulizia Paragrafi)
    # -----------------------------------------------------------------

    content_area = soup.find("body") or soup
    paragraphs_list = []
    abstract_lower = abstract.lower() if abstract else ""

    for p_tag in content_area.find_all("p"):
        paragraph_text = p_tag.get_text(" ", strip=True)

        if not paragraph_text or len(paragraph_text) < 20:
            continue

        # Filtro 1: Escludi l'Abstract
        if abstract and (paragraph_text.lower().startswith(abstract_lower) or abstract_lower.startswith(paragraph_text.lower())):
            continue

        # Filtro 2: Escludi le intestazioni
        if re.match(r'^\s*(\d+\.?\s+[A-Z][a-z]+|References|Appendix|I\.\s|II\.\s|III\.\s)', paragraph_text):
            continue

        # FILTRO 3: Rimuove i metadati di conversione e affiliazione
        paragraph_text = re.sub(r'\[.*?\]', '', paragraph_text, flags=re.DOTALL).strip()
        if re.search(r'(organization=|addressline=|city=|country=|@)', paragraph_text, re.IGNORECASE):
            paragraph_text = ""

        if paragraph_text:
            paragraphs_list.append(paragraph_text)

    paragraphs_content = " ".join(paragraphs_list)
    content_full = soup.get_text(" ", strip=True)

    if not paragraphs_content and content_full:
        paragraphs_content = content_full

    return {
        "title": title,
        "authors": authors_str,
        "date": date,
        "abstract": abstract,
        "paragraphs": paragraphs_content,
        "content_full": content_full,
        "file_path": filepath
    }

# ============================================================
# 5. INDICIZZAZIONE DOCUMENTI
# ============================================================

def index_document(doc):
    """Indicizza il singolo documento in Elasticsearch."""
    doc_id = hashlib.md5(doc["file_path"].encode()).hexdigest()
    try:
        ES.index(index=INDEX_NAME, id=doc_id, document=doc)
    except Exception as e:
        print(f"[ERRORE] Indicizzazione file {doc['file_path']} fallita: {e}")

def index_directory(path, source):
    """Processa e indicizza tutti i file HTML in una directory."""
    html_files = glob.glob(os.path.join(path, "*.html"))
    print(f"\nIndicizzazione cartella: {path}")
    print(f"File trovati: {len(html_files)}\n")

    indexed_count = 0
    for file in html_files:
        doc = parse_html(file)
        if not doc:
            print(f"[SKIP] Impossibile parsare: {file}")
            continue

        if not doc["title"] and not doc["paragraphs"]:
            print(f"[SKIP] Documento vuoto: {file}")
            continue

        doc["source"] = source
        index_document(doc)
        indexed_count += 1
        print(f"[OK] Indicizzato: {file}")

    print(f"Indicizzazione completata per {path}. Documenti indicizzati: {indexed_count}")

# ============================================================
# 6. FUNZIONE DI TEST ISOLATO PER DEBUG
# ============================================================

def test_single_file(filepath: str):
    """Esegue il parsing su un singolo file con output di debug."""
    if not os.path.exists(filepath):
        print(f"\nERRORE: File non trovato al percorso: {filepath}")
        return

    print(f"\n*** AVVIO TEST DI PARSING ISOLATO: {filepath} ***")
    doc = parse_html(filepath)

    if not doc:
        print("\nRISULTATO FINALE: Parsing fallito o file vuoto.")
        return

    print("\n--- RISULTATI ESTRATTI ---")
    print(f"Titolo: {doc.get('title')}")
    print(f"Autori: {doc.get('authors')}")
    print(f"Data: {doc.get('date')}")
    print(f"Abstract (Prime 150 char): {doc.get('abstract', '')[:150]}...")
    print(f"Paragrafi (Prime 150 char): {doc.get('paragraphs', '')[:150]}...")

    if not doc.get('authors') or doc.get('date') is None or not doc.get('abstract'):
        print("\n--- DIAGNOSI VELOCE (Se campi vuoti) ---")
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                soup = BeautifulSoup(f.read(), "html.parser")
                print(f"Meta Autori trovati: {len(soup.find_all('meta', {'name': 'citation_author'}))}")
                print(f"Div Authors trovato: {bool(soup.find('div', {'class': 'authors'}))}")
                print(f"Blocco Abstract trovato: {bool(soup.find('blockquote', {'id': 'abstract'}) or soup.find('div', {'class': 'abstract'}))}")
                print(f"Meta Data trovato: {bool(soup.find('meta', {'name': 'citation_date'}))}")
        except Exception:
            pass
    print("\n*** FINE TEST ***")


# ============================================================
# 7. MAIN
# ============================================================

def main():
    # --- Configurazione del test ---
    # MODIFICA QUESTO PERCORSO con il file problematico
    FILE_DA_DEBUGGARE = "html_corpus/arxiv_html/2301.06264v2.html"

    # 1. ESEGUI TEST ISOLATO
    if os.path.exists(FILE_DA_DEBUGGARE):
        print("ATTENZIONE: Eseguo prima il test isolato sul file problematico.")
        test_single_file(FILE_DA_DEBUGGARE)
        input("\nPremi INVIO per continuare con l'indicizzazione completa...")
    else:
        print("ATTENZIONE: File di debug non trovato. Proseguo con l'indicizzazione completa.")

    # 2. PROSEGUI CON L'INDICIZZAZIONE
    # Se devi ricreare l'indice per applicare le modifiche al mapping:
    # ES.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    create_index()
    index_directory("html_corpus/arxiv_html", "arxiv")
    index_directory("html_corpus/pubmed_html", "pubmed")

    print("\n\n*** Indicizzazione Completata ***")

    # -------------------------------
    # Controllo rapido dei documenti
    # -------------------------------
    print("\nVisualizzo alcuni documenti indicizzati:")
    res = ES.search(index=INDEX_NAME, body={
        "query": {"match_all": {}},
        "size": 20,
        "_source": ["title", "authors", "date", "abstract", "paragraphs"]
    })

    for hit in res['hits']['hits']:
        source = hit['_source']
        print("Titolo:", source.get('title'))
        print("Autori:", source.get('authors'))
        print("Data:", source.get('date'))
        print("Abstract:", source.get('abstract', '')[:100], "...")
        print("Paragrafi:", source.get('paragraphs', '')[:500], "...")
        print("-"*50)

if __name__ == "__main__":
    main()