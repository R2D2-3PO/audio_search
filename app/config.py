class Config:
    FTP_HOST = "10.1.2.230"
    FTP_USER = "anonymous"
    FTP_PASS = ""
    FTP_ROOT = "/"
    ES_HOSTS = ["http://localhost:9200"]
    INDEX_NAME = "audio_files"
    CACHE_FILE = "cache/audio_cache.json"
    LOG_FILE = "logs/app.log"
    FLASK_HOST = "127.0.0.1"
    FLASK_PORT = 5001