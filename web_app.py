# web_app.py
from flask import Flask, request, render_template_string
from elasticsearch import Elasticsearch

ES = Elasticsearch("http://localhost:9200")
INDEX_NAME = "research_articles_v2"

ALL_FIELDS = ["title", "authors", "abstract", "paragraphs", "content_full"]
DEFAULT_FIELDS = ["title", "abstract", "paragraphs"]

app = Flask(__name__)

HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>Scientific Search</title>
</head>
<body>
    <h1>Scientific Search</h1>
    <form method="get" action="/">
        <label>Query:</label>
        <input type="text" name="q" value="{{ q or '' }}" size="60">

        <p>Campi:</p>
        {% for f in all_fields %}
            <label>
                <input type="checkbox" name="fields" value="{{ f }}"
                       {% if f in fields %}checked{% endif %}>
                {{ f }}
            </label><br>
        {% endfor %}

        <p>
            <button type="submit">Cerca</button>
        </p>
        <p style="font-size: small; color: #555;">
            Supporta AND, OR, NOT, virgolette. Es: (entity OR record) AND resolution
        </p>
    </form>

    {% if results is not none %}
        <h2>Risultati: {{ results|length }}</h2>
        <hr>
        {% for r in results %}
            <div style="margin-bottom: 1.5em;">
                <strong>{{ r.title }}</strong><br>
                <em>{{ r.authors }}</em><br>
                <span>{{ r.date }} — {{ r.source }}</span><br>
                <p>{{ r.snippet }}...</p>
            </div>
            <hr>
        {% endfor %}
    {% endif %}
</body>
</html>
"""


def es_search(query, fields, size=20):
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
    results = []
    for h in resp["hits"]["hits"]:
        src = h["_source"]
        snippet = (src.get("abstract") or src.get("paragraphs") or "")[:300]
        results.append({
            "title": src.get("title", ""),
            "authors": src.get("authors", ""),
            "date": src.get("date", ""),
            "source": src.get("source", ""),
            "snippet": snippet.replace("\n", " ")
        })
    return results


@app.route("/", methods=["GET"])
def home():
    q = request.args.get("q", "").strip()
    fields = request.args.getlist("fields")
    if not fields:
        fields = DEFAULT_FIELDS

    results = None
    if q:
        results = es_search(q, fields)

    return render_template_string(
        HTML_TEMPLATE,
        q=q,
        fields=fields,
        all_fields=ALL_FIELDS,
        results=results
    )


if __name__ == "__main__":
    app.run(debug=True)
