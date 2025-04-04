import os
from ftplib import FTP
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor
import json
import os.path

# 配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
es = Elasticsearch(["http://localhost:9200"])
index_name = "audio_files"
FTP_HOST = "10.1.2.230"
FTP_USER = "anonymous"
FTP_PASS = ""
FTP_ROOT = "/"
CACHE_FILE = "audio_cache.json"

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
                    "fuzziness": "AUTO",  # 支持模糊匹配
                    "operator": "and"
                }
            },
            "sort": [{sort_by: {"order": order}}],
            "size": size  # 默认返回 20 条结果
        }
        response = self.es.search(index=self.index_name, body=body)
        return response["hits"]["hits"]

def format_search_results(hits):
    results = []
    for hit in hits:
        source = hit["_source"]
        results.append(f"File: {source['file_name']}, Path: {source['ftp_path']}, "
                      f"Size: {source['size']} bytes, Modified: {source['modified']}")
    return results

def interactive_search(es_manager):
    print("Welcome to Audio File Search (type 'exit' to quit)")
    while True:
        query = input("Enter search query (e.g., 'wind modern'): ")
        if query.lower() == "exit":
            break
        sort_by = input("Sort by (size/modified, default: size): ") or "size"
        order = input("Order (asc/desc, default: asc): ") or "asc"
        hits = es_manager.search(query, sort_by=sort_by, order=order)
        results = format_search_results(hits)
        if results:
            print(f"\nFound {len(results)} results:")
            for i, result in enumerate(results, 1):
                print(f"{i}. {result}")
        else:
            print("No results found.")

def main():
    ftp = connect_ftp()
    es_manager = ElasticsearchManager(es, index_name)
    try:
        audio_files = extract_audio_info(ftp)
        es_manager.index_data(audio_files)
        interactive_search(es_manager)
    except Exception as e:
        logging.error(f"Main process failed: {e}")
    finally:
        ftp.quit()

if __name__ == "__main__":
    main()