import socket
from ftplib import FTP
from elasticsearch import Elasticsearch
import logging
from .config import Config

def check_ftp_connection():
    try:
        ftp = FTP(Config.FTP_HOST, timeout=5)
        ftp.login(Config.FTP_USER, Config.FTP_PASS)
        ftp.quit()
        return True
    except Exception as e:
        logging.warning(f"FTP connection failed: {e}")
        return False

def check_es_connection():
    try:
        es = Elasticsearch(Config.ES_HOSTS, timeout=5)
        return es.ping()
    except Exception as e:
        logging.warning(f"Elasticsearch connection failed: {e}")
        return False