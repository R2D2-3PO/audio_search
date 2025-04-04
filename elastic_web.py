import os
from ftplib import FTP
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor
import json
import os.path
from flask import Flask, request, render_template_string

# 配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
es = Elasticsearch(["http://localhost:9200"])
index_name = "audio_files"
FTP_HOST = "10.1.2.230"
FTP_USER = "anonymous"
FTP_PASS = ""
FTP_ROOT = "/"
CACHE_FILE = "audio_cache.json"

app = Flask(__name__)

def connect_ftp():
    ftp = FTP(FTP_HOST)
    ftp.encoding = "gbk"
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def list_ftp_dir(ftp, dir_path):
    files = []
    dirs = []
    logging.info(f"Scanning directory: {dir_path}")
    try:
        ftp.cwd(dir_path)
        items = ftp.nlst()
    except Exception as e:
        logging.error(f"Error accessing {dir_path}: {e}")
        return files

    for item in items:
        if item in ('.', '..'):
            continue
        try:
            ftp.cwd(f"{dir_path}/{item}".replace("//", "/"))
            dirs.append(item)
            ftp.cwd(dir_path)
        except:
            if item.endswith(".wav"):
                files.append((dir_path, item))

    for sub_dir in dirs:
        sub_path = f"{dir_path}/{sub_dir}".replace("//", "/")
        sub_files = list_ftp_dir(ftp, sub_path)
        files.extend(sub_files)

    logging.info(f"Found {len(files)} .wav files in {dir_path}")
    return files

def process_ftp_file(full_path):
    ftp = connect_ftp()
    try:
        full_path = full_path.replace("//", "/").strip()
        logging.info(f"Processing metadata for: {full_path}")
        size = ftp.size(full_path)
        if size is None:
            logging.warning(f"File not found via ftp.size: {full_path}")
            return None
        modified = ftp.voidcmd(f"MDTM {full_path}")[4:].strip()
        info = {
            "file_name": os.path.basename(full_path),
            "ftp_path": full_path,
            "size": size,
            "modified": modified
        }
        return info
    except Exception as e:
        logging.error(f"Failed to process {full_path}: {e}")
        return None
    finally:
        ftp.quit()

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)

def extract_audio_info(ftp_main):
    audio_data = []
    wav_files = list_ftp_dir(ftp_main, FTP_ROOT)
    cache = load_cache()
    logging.info(f"Total .wav files found: {len(wav_files)}")
    logging.info(f"Loaded {len(cache)} items from cache")

    def process_with_cache(args):
        dir_path, file = args
        full_path = f"{dir_path}/{file}".replace("//", "/")
        ftp = connect_ftp()
        try:
            modified = ftp.voidcmd(f"MDTM {full_path}")[4:].strip()
            if full_path in cache and cache[full_path]["modified"] == modified:
                logging.info(f"Using cached data for {full_path}")
                return cache[full_path]
            else:
                info = process_ftp_file(full_path)
                if info:
                    logging.info(f"Processed and cached {full_path}")
                    cache[full_path] = info
                return info
        finally:
            ftp.quit()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_with_cache, (dir_path, file)) for dir_path, file in wav_files]
        for future in tqdm(futures, desc="Processing audio files"):
            info = future.result()
            if info:
                audio_data.append(info)

    save_cache(cache)
    logging.info(f"Saved cache with {len(cache)} items")
    return audio_data

class ElasticsearchManager:
    def __init__(self, es_client, index_name):
        self.es = es_client
        self.index_name = index_name
        self._ensure_index()

    def _ensure_index(self):
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "file_name": {"type": "text"},
                        "ftp_path": {"type": "text"},
                        "size": {"type": "long"},
                        "modified": {"type": "date", "format": "yyyyMMddHHmmss"}
                    }
                }
            }
            self.es.indices.create(index=self.index_name, body=mapping)
            logging.info(f"Created index {self.index_name}")

    def index_data(self, data):
        actions = [
            {
                "_index": self.index_name,
                "_id": i,
                "_source": item
            }
            for i, item in enumerate(data) if item
        ]
        bulk(self.es, actions)
        logging.info(f"Indexed {len(data)} audio files")

    def search(self, query, size=20, sort_by="size", order="asc"):
        body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["file_name", "ftp_path"],
                    "fuzziness": "AUTO",
                    "operator": "and"
                }
            },
            "sort": [{sort_by: {"order": order}}],
            "size": size
        }
        response = self.es.search(index=self.index_name, body=body)
        return response["hits"]["hits"]

def format_search_results(hits):
    results = []
    for hit in hits:
        source = hit["_source"]
        results.append({
            "file_name": source["file_name"],
            "ftp_path": source["ftp_path"],
            "size": source["size"],
            "modified": source["modified"]
        })
    return results

# Flask 路由
@app.route('/', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        query = request.form.get('query', '')
        sort_by = request.form.get('sort_by', 'size')
        order = request.form.get('order', 'asc')
        es_manager = ElasticsearchManager(es, index_name)
        hits = es_manager.search(query, sort_by=sort_by, order=order)
        results = format_search_results(hits)
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
            button { padding: 8px 16px; }
            .result { margin: 10px 0; }
        </style>
    </head>
    <body>
        <h1>Audio File Search</h1>
        <form method="post">
            <input type="text" name="query" value="{{ query }}" placeholder="Enter search query (e.g., 'wind modern')">
            <select name="sort_by">
                <option value="size" {% if sort_by == 'size' %}selected{% endif %}>Size</option>
                <option value="modified" {% if sort_by == 'modified' %}selected{% endif %}>Modified</option>
            </select>
            <select name="order">
                <option value="asc" {% if order == 'asc' %}selected{% endif %}>Ascending</option>
                <option value="desc" {% if order == 'desc' %}selected{% endif %}>Descending</option>
            </select>
            <button type="submit">Search</button>
        </form>
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
    return render_template_string(html, query=query, sort_by=sort_by, order=order, results=results)

def main():
    ftp = connect_ftp()
    es_manager = ElasticsearchManager(es, index_name)
    try:
        audio_files = extract_audio_info(ftp)
        es_manager.index_data(audio_files)
        logging.info("Starting web server...")
        app.run(debug=True, host='0.0.0.0', port=5001)
    except Exception as e:
        logging.error(f"Main process failed: {e}")
    finally:
        ftp.quit()

if __name__ == "__main__":
    main()