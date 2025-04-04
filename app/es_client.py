from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging
from .config import Config

class ESClient:
    def __init__(self, synonyms=None):
        self.es = Elasticsearch(Config.ES_HOSTS)
        self.index_name = Config.INDEX_NAME
        self.synonyms = synonyms or []
        self._ensure_index()

    def _ensure_index(self):
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
            logging.info(f"Deleted existing index {self.index_name}")

        settings = {
            "settings": {
                "analysis": {
                    "filter": {
                        "synonym_filter": {
                            "type": "synonym",
                            "synonyms": self.synonyms
                        }
                    },
                    "analyzer": {
                        "synonym_analyzer": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "synonym_filter"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "file_name": {"type": "text", "analyzer": "synonym_analyzer"},
                    "ftp_path": {"type": "text", "analyzer": "synonym_analyzer"},
                    "size": {"type": "long"},
                    "modified": {"type": "date", "format": "yyyyMMddHHmmss"}
                }
            }
        }
        self.es.indices.create(index=self.index_name, body=settings)
        logging.info(f"Created index {self.index_name} with synonym support")

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
                    "operator": "and",
                    "analyzer": "synonym_analyzer"
                }
            },
            "sort": [{sort_by: {"order": order}}],
            "size": size
        }
        response = self.es.search(index=self.index_name, body=body)
        return response["hits"]["hits"]