import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from .ftp_client import FTPClient
from .config import Config
import ollama
from collections import Counter
import re

class AudioProcessor:
    def __init__(self):
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(Config.CACHE_FILE):
            with open(Config.CACHE_FILE, 'r') as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        if not os.path.exists("cache"):
            os.makedirs("cache")
        with open(Config.CACHE_FILE, 'w') as f:
            json.dump(self.cache, f)

    def process_files(self):
        ftp_main = FTPClient()
        try:
            wav_files = ftp_main.list_dir(Config.FTP_ROOT)
            logging.info(f"Total .wav files found: {len(wav_files)}")
            audio_data = self._process_parallel(wav_files)
            self._save_cache()
            return audio_data
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
                model="llama3",
                messages=[
                    {"role": "user", "content": f"将以下中文短语转换为适合搜索的英文关键词（简洁且相关）：{chinese_input}"}
                ]
            )
            keywords = response["message"]["content"].strip()
            logging.info(f"Translated '{chinese_input}' to '{keywords}'")
            return keywords
        except Exception as e:
            logging.error(f"Failed to translate '{chinese_input}': {e}")
            return chinese_input

    def generate_synonym_table(self, audio_data, top_n=10):
        """从文件元数据生成近义词表"""
        # 提取文件名中的单词
        all_words = []
        for item in audio_data:
            # 简单分词：按空格、连字符、下划线分割，去除数字和扩展名
            words = re.split(r'[\s\-_]+', item["file_name"].lower().replace(".wav", ""))
            all_words.extend([w for w in words if w.isalpha() and len(w) > 2])

        # 统计高频词
        word_freq = Counter(all_words)
        top_words = [word for word, _ in word_freq.most_common(top_n)]
        logging.info(f"Top {top_n} frequent words: {top_words}")

        # 使用 Ollama 生成近义词
        synonym_table = []
        for word in top_words:
            try:
                response = ollama.chat(
                    model="llama3",
                    messages=[
                        {"role": "user", "content": f"Provide synonyms for '{word}' in English (comma-separated)."}
                    ]
                )
                synonyms = response["message"]["content"].strip()
                synonym_entry = f"{word} => {synonyms}"
                synonym_table.append(synonym_entry)
                logging.info(f"Generated synonym: {synonym_entry}")
            except Exception as e:
                logging.error(f"Failed to generate synonyms for '{word}': {e}")

        return synonym_table