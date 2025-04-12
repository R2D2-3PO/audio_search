from flask import request, render_template_string
from .es_client import ESClient
from .processor import AudioProcessor

def init_routes(app):
    processor = AudioProcessor()

    @app.route('/', methods=['GET', 'POST'])
    def search():
        es_client = ESClient()
        translated_query = None  # 翻译结果

        if request.method == 'POST':
            query_type = request.form.get('query_type', 'normal')
            query = request.form.get('query', '')  # 用户原始输入
            sort_by = request.form.get('sort_by', 'size')
            order = request.form.get('order', 'asc')

            if query_type == 'chinese':
                # 中文搜索：翻译为英文关键词，但不覆盖 query
                translated_query = processor.translate_to_keywords(query)
                search_query = translated_query
            else:
                # 普通搜索：直接使用用户输入
                search_query = query

            hits = es_client.search(search_query, sort_by=sort_by, order=order)
            results = [
                {
                    "file_name": hit["_source"]["file_name"],
                    "ftp_path": hit["_source"]["ftp_path"],
                    "size": hit["_source"]["size"],
                    "modified": hit["_source"]["modified"]
                }
                for hit in hits
            ]
        else:
            results = []
            query = ''
            sort_by = 'size'
            order = 'asc'

        html = '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Audio File Search</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                input[type="text"] { width: 50%; padding: 8px; }
                button { padding: 8px 16px; margin: 0 5px; }
                .result { margin: 10px 0; }
                .translated { color: #555; font-style: italic; margin: 10px 0; }
            </style>
        </head>
        <body>
            <h1>Audio File Search</h1>
            <form method="post">
                <input type="text" name="query" value="{{ query }}" placeholder="Enter search query (e.g., 'wind modern' or '风吹过现代建筑')">
                <select name="sort_by">
                    <option value="size" {% if sort_by == 'size' %}selected{% endif %}>Size</option>
                    <option value="modified" {% if sort_by == 'modified' %}selected{% endif %}>Modified</option>
                </select>
                <select name="order">
                    <option value="asc" {% if order == 'asc' %}selected{% endif %}>Ascending</option>
                    <option value="desc" {% if order == 'desc' %}selected{% endif %}>Descending</option>
                </select>
                <button type="submit" name="query_type" value="normal">Search</button>
                <button type="submit" name="query_type" value="chinese">智能搜索</button>
            </form>
            {% if translated_query %}
                <div class="translated">Translated to: {{ translated_query }}</div>
            {% endif %}
            {% if results %}
                <h2>Results ({{ results|length }} found):</h2>
                {% for result in results %}
                    <div class="result">
                        <strong>{{ result.file_name }}</strong><br>
                        Path: {{ result.ftp_path }}<br>
                         Size: {{ result.size }} bytes<br>
                        Modified: {{ result.modified }}
                    </div>
                {% endfor %}
            {% endif %}
        </body>
        </html>
        '''
        return render_template_string(html, query=query, sort_by=sort_by, order=order, results=results, translated_query=translated_query)