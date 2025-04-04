import os
from ftplib import FTP
from io import BytesIO
import librosa
from elasticsearch import Elasticsearch

# Elasticsearch 连接
es = Elasticsearch(["http://localhost:9200"])
index_name = "audio_files"

# FTP 配置
FTP_HOST = "10.1.2.230"
FTP_USER = "anonymous"
FTP_PASS = ""
FTP_ROOT = "/"  # 从根目录开始递归


def connect_ftp():
    ftp = FTP(FTP_HOST)
    ftp.encoding = "latin1"  # 宽松解码，避免 UnicodeDecodeError
    ftp.login(FTP_USER, FTP_PASS)
    return ftp


def test_ftp_connection():
    ftp = connect_ftp()
    print(f"Connected to {FTP_HOST}, current dir: {ftp.pwd()}")
    lines = []
    ftp.retrlines('LIST', lines.append)
    print("Directory listing:")
    for line in lines:
        print(line)
    ftp.quit()

# 在 main() 之前调用
def main():
    test_ftp_connection()


if __name__ == "__main__":
    main()