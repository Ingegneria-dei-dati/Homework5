import requests
import feedparser
import time
import json
import os

# --- 1. Configurazione della Ricerca e Output ---
# La query cerca "Entity resolution" O "Entity matching" nell'abstract o nel titolo
SEARCH_QUERY = 'all:("Entity resolution" OR "Entity matching")'
BASE_URL = 'http://export.arxiv.org/api/query?'
MAX_RESULTS_PER_PAGE = 100
TOTAL_MAX_RESULTS = 500  # Numero massimo di articoli da recuperare (modifica se necessario)
OUTPUT_FILE = 'corpus_metadata.json' # Il file verrà salvato nella radice del progetto
# -----------------------------------------------

def get_articles_from_arxiv():
    """
    Recupera i metadati degli articoli da arXiv in base a SEARCH_QUERY e li salva in un file JSON.
    """
    corpus = []
    start_index = 0

    print(f"Avvio della ricerca per: {SEARCH_QUERY}")

    while start_index < TOTAL_MAX_RESULTS:
        # Costruzione dei parametri per la richiesta API
        query_params = {
            'search_query': SEARCH_QUERY,
            'start': start_index,
            'max_results': MAX_RESULTS_PER_PAGE,
            'sortBy': 'submittedDate',
            'sortOrder': 'descending',
            'id_list': ''
        }

        try:
            # Invio della richiesta HTTP
            response = requests.get(BASE_URL, params=query_params)
            response.raise_for_status() # Solleva un errore per codici di stato 4xx/5xx
        except requests.exceptions.RequestException as e:
            print(f"ERRORE nella richiesta HTTP: {e}")
            break

        # Parsing della risposta XML/Atom
        feed = feedparser.parse(response.text)

        if not feed.entries:
            print(f"Nessun altro risultato trovato dopo l'indice {start_index}. Totale recuperato: {len(corpus)}")
            break

        print(f"-> Recuperati {len(feed.entries)} articoli (indice di partenza: {start_index})")

        for entry in feed.entries:
            # Estrazione e pulizia dei dati
            arxiv_id = entry.id.split('/abs/')[-1]
            title = entry.title.replace('\n', ' ').strip()
            abstract = entry.summary.replace('\n', ' ').strip()

            # Trova l'URL del PDF
            pdf_url = next((link['href'] for link in entry.links if link.get('type') == 'application/pdf'), None)

            if pdf_url:
                corpus.append({
                    'arxiv_id': arxiv_id,
                    'title': title,
                    'abstract': abstract,
                    'pdf_url': pdf_url
                })

        # Aggiornamento dell'indice per la prossima "pagina" di risultati
        start_index += MAX_RESULTS_PER_PAGE

        # Pausa per rispettare il limite di richieste di arXiv
        time.sleep(3)

        # --- 2. Salvataggio del Corpus in JSON ---
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(corpus, f, ensure_ascii=False, indent=4)

        full_path = os.path.abspath(OUTPUT_FILE)
        print(f"\nMetadati salvati in {OUTPUT_FILE} (Totale: {len(corpus)} articoli).")
        print(f"Percorso assoluto del file: {full_path}")
    except IOError as e:
        print(f"ERRORE nel salvataggio del file JSON: {e}")

    return corpus

if __name__ == '__main__':
    # Ricorda di eseguire: pip install requests feedparser
    get_articles_from_arxiv()