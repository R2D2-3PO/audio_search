import os
from ftplib import FTP
from io import BytesIO
import librosa
from elasticsearch import Elasticsearch

es = Elasticsearch(["http://localhost:9200"])
index_name = "audio_files"

FTP_HOST = "10.1.2.230"
FTP_USER = "anonymous"
FTP_PASS = ""
FTP_ROOT = "/"


def connect_ftp():
    ftp = FTP(FTP_HOST)
    ftp.encoding = "gbk"  # 尝试 GBK 编码，常见于中文环境
    ftp.login(FTP_USER, FTP_PASS)
    return ftp


def list_ftp_dir(ftp, dir_path):
    files = []
    dirs = []
    print(f"Scanning directory: {dir_path}")
    lines = []
    try:
        ftp.cwd(dir_path)
        ftp.retrlines('LIST', lines.append)
    except Exception as e:
        print(f"Error accessing {dir_path}: {e}")
        return files

    for line in lines:
        print(f"Raw line: {line}")
        if "<DIR>" in line:
            is_dir = True
            name_start = line.index("<DIR>") + len("<DIR>")
        else:
            is_dir = False
            name_start = 27
        name = line[name_start:].strip()

        if is_dir and name not in ('.', '..'):
            dirs.append(name)
        elif name.endswith(".wav"):
            files.append((dir_path, name))

    for sub_dir in dirs:
        sub_path = f"{dir_path}/{sub_dir}".replace("//", "/")
        sub_files = list_ftp_dir(ftp, sub_path)
        files.extend(sub_files)

    print(f"Found {len(files)} .wav files in {dir_path}")
    return files


def extract_audio_info(ftp):
    audio_data = []
    wav_files = list_ftp_dir(ftp, FTP_ROOT)
    print(f"Total .wav files found: {len(wav_files)}")

    for i, (dir_path, file) in enumerate(wav_files, 1):
        full_path = f"{dir_path}/{file}".replace("//", "/")
        print(f"Processing {i}/{len(wav_files)}: {full_path}")
        audio_buffer = BytesIO()
        try:
            # 调试：打印实际发送的 FTP 命令
            ftp.retrbinary(f"RETR {full_path}", audio_buffer.write)
            audio_buffer.seek(0)
            duration = librosa.get_duration(fileobj=audio_buffer)
            info = {
                "file_name": file,
                "ftp_path": full_path,
                "duration": duration
            }
            audio_data.append(info)
            print(f"Success: {full_path} (duration: {duration}s)")
        except Exception as e:
            print(f"Error processing {full_path}: {e}")
        finally:
            audio_buffer.close()
    return audio_data


def index_audio_files(audio_data):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    for i, audio in enumerate(audio_data):
        es.index(index=index_name, id=i, document=audio)
    print(f"Indexed {len(audio_data)} audio files.")


def search_audio(query):
    body = {"query": {"multi_match": {"query": query, "fields": ["file_name", "ftp_path"]}}}
    response = es.search(index=index_name, body=body)
    return response["hits"]["hits"]


def check_all_indexed():
    body = {"query": {"match_all": {}}}
    response = es.search(index=index_name, body=body, size=10)
    print("Total indexed documents:", response["hits"]["total"]["value"])
    for hit in response["hits"]["hits"]:
        print(hit["_source"])


def main():
    ftp = connect_ftp()
    audio_files = extract_audio_info(ftp)
    for audio in audio_files:
        print(audio)
    index_audio_files(audio_files)
    results = search_audio("test")
    for hit in results:
        print(hit["_source"])
    check_all_indexed()
    ftp.quit()


if __name__ == "__main__":
    main()