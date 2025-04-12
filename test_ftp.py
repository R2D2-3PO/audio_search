import os
from ftplib import FTP
from io import BytesIO
import librosa
from elasticsearch import Elasticsearch

# Elasticsearch 连接
es = Elasticsearch(["http://localhost:9200"])
index_name = "audio_files"

# FTP 配置
FTP_HOST = "192.168.100.99"
FTP_USER = "ai"
FTP_PASS = "Dykys#Gaoxin#10099"
FTP_ROOT = "/media/ai/sound_effects"  # 从根目录开始递归


def connect_ftp():
    ftp = FTP(FTP_HOST)
    ftp.encoding = "latin1"  # 宽松解码，避免 UnicodeDecodeError
    ftp.login(FTP_USER, FTP_PASS)
    return ftp


def test_ftp_connection():
    ftp = connect_ftp()
    print(f"Connected to {FTP_HOST}, current dir: {ftp.pwd()}")

    # 切换到 FTP_ROOT 目录
    try:
        ftp.cwd(FTP_ROOT)
        print(f"Changed directory to: {ftp.pwd()}")
    except Exception as e:
        print(f"Failed to change directory to {FTP_ROOT}: {e}")
        ftp.quit()
        return

    # 列出当前目录内容
    lines = []
    ftp.retrlines('LIST', lines.append)
    print(f"Directory listing for {FTP_ROOT}:")
    for line in lines:
        print(line)

    ftp.quit()


def main():
    test_ftp_connection()


if __name__ == "__main__":
    main()