# extract_tables.py
import re
from bs4 import BeautifulSoup

# Stopwords minime per non-informative terms (puoi ampliarle se vuoi)
STOPWORDS = {
    "the", "and", "of", "in", "to", "for", "on", "at", "by", "a", "an",
    "is", "are", "was", "were", "with", "as", "that", "this", "from",
    "we", "our", "their", "its", "be", "or", "it", "not", "may", "can"
}


def tokenize(text):
    """Tokenizzazione semplice + rimozione stopwords e numeri."""
    tokens = re.findall(r"\b\w+\b", text.lower())
    cleaned = [
        t for t in tokens
        if t not in STOPWORDS and not t.isdigit() and len(t) > 2
    ]
    return cleaned


def extract_paragraphs(soup):
    """Ritorna la lista dei paragrafi (stringhe) dal documento."""
    paragraphs = []
    for p in soup.find_all("p"):
        txt = p.get_text(" ", strip=True)
        if txt:
            paragraphs.append(txt)
    return paragraphs


def find_caption_for_table(table_tag):
    """
    Cerca la caption di una tabella:
    1) <caption> dentro la tabella
    2) un elemento vicino con class 'caption' o simile
    """
    cap = table_tag.find("caption")
    if cap:
        return cap.get_text(" ", strip=True)

    # Alcuni HTML (es. PMC) mettono la caption fuori, es. <div class="caption">
    # Proviamo a vedere il genitore o il fratello
    parent = table_tag.parent
    if parent:
        cap_div = parent.find("div", class_=re.compile("caption", re.I))
        if cap_div:
            return cap_div.get_text(" ", strip=True)

    # fallback
    return ""


def guess_table_number(table_tag, index_fallback):
    """
    Cerca di indovinare il numero della tabella (per costruire 'Table 1', ...).
    1) guarda l'attributo id
    2) guarda la caption
    3) altrimenti usa l'indice di fallback
    """
    # 1) id del tag
    tid = table_tag.get("id") or table_tag.get("name")
    if tid:
        m = re.search(r'(\d+)', tid)
        if m:
            return m.group(1)

    # 2) cerca pattern nella caption
    cap = find_caption_for_table(table_tag)
    m = re.search(r'[Tt]able\s+(\d+)', cap)
    if m:
        return m.group(1)

    # 3) fallback
    return str(index_fallback)


def extract_tables_from_html(html: str, paper_id: str):
    """
    Estrae tutte le tabelle dal documento HTML con il loro contesto.
    Ritorna una lista di dict:
    {
      "paper_id": ...,
      "table_id": ...,
      "caption": ...,
      "body": ...,
      "mentions": [...],
      "context_paragraphs": [...]
    }
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) prendi tutti i paragrafi del paper una volta sola
    paragraphs = extract_paragraphs(soup)

    # 2) trova tutte le <table>
    tables = []
    for idx, table_tag in enumerate(soup.find_all("table"), start=1):
        table_num = guess_table_number(table_tag, idx)
        table_id = table_tag.get("id") or f"table_{table_num}"

        # CAPTION
        caption = find_caption_for_table(table_tag)

        # BODY della tabella: concateno righe/celle
        rows_text = []
        for tr in table_tag.find_all("tr"):
            row_txt = tr.get_text(" ", strip=True)
            if row_txt:
                rows_text.append(row_txt)
        body_text = " ".join(rows_text) or table_tag.get_text(" ", strip=True)

        # MENTIONS: paragrafi che citano esplicitamente "Table X" o "Tab. X"
        mention_patterns = [
            re.compile(rf"\b[Tt]able\s*{table_num}\b"),
            re.compile(rf"\b[Tt]ab\.?\s*{table_num}\b"),
        ]
        mentions = []
        for par in paragraphs:
            if any(p.search(par) for p in mention_patterns):
                mentions.append(par)

        # CONTEXT_PARAGRAPHS:
        # paragrafi che contengono termini (informativi) presenti in caption/body
        key_terms = set(tokenize(caption) + tokenize(body_text))
        context_paragraphs = []
        if key_terms:
            for par in paragraphs:
                # Evita di duplicare i paragrafi che sono già nelle mentions
                if par in mentions:
                    continue
                par_terms = set(tokenize(par))
                # soglia minimale: almeno 2 termini in comune
                if len(key_terms & par_terms) >= 2:
                    context_paragraphs.append(par)

        tables.append({
            "paper_id": paper_id,
            "table_id": table_id,
            "caption": caption,
            "body": body_text,
            "mentions": mentions,
            "context_paragraphs": context_paragraphs,
        })

    return tables
