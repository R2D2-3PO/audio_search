from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging
from .config import Config

class ESClient:
    def __init__(self):
        self.es = Elasticsearch(Config.ES_HOSTS)
        self.index_name = Config.INDEX_NAME
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