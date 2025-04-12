import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from .ftp_client import FTPClient
from .config import Config
from .utils import check_ftp_connection  # 新增导入
import ollama
from collections import Counter
import re

class AudioProcessor:
    def __init__(self):
        self.cache = self._load_cache()

    def _load_cache(self):
        try:
            if os.path.exists(Config.CACHE_FILE):
                with open(Config.CACHE_FILE, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logging.error(f"Failed to load cache: {e}")
            return {}

    def _save_cache(self):
        try:
            if not os.path.exists("cache"):
                os.makedirs("cache")
            with open(Config.CACHE_FILE, 'w') as f:
                json.dump(self.cache, f)
        except Exception as e:
            logging.error(f"Failed to save cache: {e}")

    def process_files(self):
        # 检查 FTP 连接
        if not check_ftp_connection():
            logging.warning("FTP connection unavailable, using cached data")
            # 从缓存加载音频数据
            audio_data = list(self.cache.values())
            if not audio_data:
                logging.error("No cached data available")
                return []
            logging.info(f"Loaded {len(audio_data)} files from cache")
            return audio_data

        # 在线模式：连接 FTP 获取数据
        ftp_main = FTPClient()
        try:
            wav_files = ftp_main.list_dir(Config.FTP_ROOT)
            logging.info(f"Total .wav files found: {len(wav_files)}")
            audio_data = self._process_parallel(wav_files)
            self._save_cache()
            return audio_data
        except Exception as e:
            logging.error(f"Failed to process files from FTP: {e}")
            # 回退到缓存
            audio_data = list(self.cache.values())
            if audio_data:
                logging.info(f"Fallback to {len(audio_data)} cached files")
                return audio_data
            raise
        finally:
            ftp_main.close()

    def _process_parallel(self, wav_files):
        audio_data = []

        def process_with_cache(args):
            dir_path, file = args
            full_path = f"{dir_path}/{file}".replace("//", "/")
            ftp = FTPClient()
            try:
                modified = ftp.get_metadata(full_path)["modified"]
                if full_path in self.cache and self.cache[full_path]["modified"] == modified:
                    logging.info(f"Using cached data for {full_path}")
                    return self.cache[full_path]
                else:
                    metadata = ftp.get_metadata(full_path)
                    if metadata:
                        info = {
                            "file_name": os.path.basename(full_path),
                            "ftp_path": full_path,
                            "size": metadata["size"],
                            "modified": metadata["modified"]
                        }
                        logging.info(f"Processed and cached {full_path}")
                        self.cache[full_path] = info
                        return info
                    return None
            finally:
                ftp.close()

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_with_cache, (dir_path, file)) for dir_path, file in wav_files]
            for future in tqdm(futures, desc="Processing audio files"):
                info = future.result()
                if info:
                    audio_data.append(info)

        return audio_data

    def translate_to_keywords(self, chinese_input):
        try:
            response = ollama.chat(
                model="llama3.2:1b",
                messages=[
                    {"role": "user", "content": f"将以下中文短语转换为适合搜索的英文关键词（简洁且相关）：{chinese_input}"}
                ],
                options={"timeout": 10}
            )
            keywords = response["message"]["content"].strip()
            logging.info(f"Translated '{chinese_input}' to '{keywords}'")
            return keywords
        except Exception as e:
            logging.error(f"Failed to translate '{chinese_input}': {e}")
            return chinese_input

    def generate_synonym_table(self, audio_data, top_n=10):
        # 提取文件名中的单词
        all_words = []
        for item in audio_data:
            words = re.split(r'[\s\-_]+', item["file_name"].lower().replace(".wav", ""))
            all_words.extend([w for w in words if w.isalpha() and len(w) > 2])

        # 统计高频词
        word_freq = Counter(all_words)
        top_words = [word for word, _ in word_freq.most_common(top_n)]
        logging.info(f"Top {top_n} frequent words: {top_words}")

        synonym_table = []
        for word in top_words:
            try:
                response = ollama.chat(
                    model="llama3.2:1b",
                    messages=[
                        {"role": "user", "content": f"Provide synonyms for '{word}' in English (comma-separated)."}],
                    options={"timeout": 10}
                )
                synonyms = response["message"]["content"].strip()
                synonym_table.append(f"{word} => {synonyms}")
                logging.info(f"Generated synonym: {word} => {synonyms}")
            except Exception as e:
                logging.error(f"Failed to generate synonyms for '{word}': {e}")
                synonym_table.append(f"{word} => {word}")
        return synonym_table

    def local_search(self, query, size=20, sort_by="size", order="asc"):
        """离线搜索：基于缓存数据进行简单匹配"""
        audio_data = list(self.cache.values())
        if not audio_data:
            logging.warning("No cached data for local search")
            return []

        # 简单关键词匹配
        query = query.lower()
        results = [
            item for item in audio_data
            if query in item["file_name"].lower() or query in item["ftp_path"].lower()
        ]

        # 排序
        reverse = (order == "desc")
        if sort_by == "size":
            results.sort(key=lambda x: x["size"], reverse=reverse)
        elif sort_by == "modified":
            results.sort(key=lambda x: x["modified"], reverse=reverse)

        # 限制结果数量
        return results[:size]