# search_cli.py
from elasticsearch import Elasticsearch

ES = Elasticsearch("http://localhost:9200")
INDEX_NAME = "research_articles_v2"

DEFAULT_FIELDS = ["title", "abstract", "paragraphs"]

def run_search(query, fields=None, size=10):
    """
    Esegue una ricerca full-text/booleana su uno o più campi.
    Usa query_string, quindi supporta AND, OR, NOT, virgolette, ecc.
    """
    if not fields:
        fields = DEFAULT_FIELDS

    body = {
        "query": {
            "query_string": {
                "query": query,
                "fields": fields
            }
        },
        "size": size
    }

    resp = ES.search(index=INDEX_NAME, body=body)
    return resp["hits"]["hits"]


def main():
    print("=== SHELL DI RICERCA (Elasticsearch) ===")
    print("Campi disponibili: title, authors, abstract, paragraphs, content_full")
    print("Esempi di query:")
    print('  entity AND resolution')
    print('  "entity matching"')
    print('  (entity OR record) AND resolution')
    print("---------------------------------------\n")

    while True:
        q = input("🔎 Query (invio vuoto per uscire): ").strip()
        if not q:
            print("Bye!")
            break

        fields_raw = input(
            "Campi (es: title,abstract,paragraphs) [default: title,abstract,paragraphs]: "
        ).strip()

        if fields_raw:
            fields = [f.strip() for f in fields_raw.split(",") if f.strip()]
        else:
            fields = DEFAULT_FIELDS

        hits = run_search(q, fields=fields, size=10)
        print(f"\nRisultati trovati: {len(hits)}")
        print("-" * 60)
        for h in hits:
            src = h["_source"]
            print(f"Score: {h['_score']:.2f}")
            print(f"Titolo:  {src.get('title')}")
            print(f"Autori:  {src.get('authors')}")
            print(f"Data:    {src.get('date')}")
            print(f"Source:  {src.get('source')}")
            # Mostra snippet dai paragrafi
            text = src.get("paragraphs", "")[:300].replace("\n", " ")
            print(f"Testo:   {text}...")
            print("-" * 60)
        print()

if __name__ == "__main__":
    main()
