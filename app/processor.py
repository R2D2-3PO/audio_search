import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from .ftp_client import FTPClient
from .config import Config
import ollama

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
        """使用 Ollama 将中文转换为英文搜索关键词"""
        try:
            # 调用本地 Ollama 服务
            response = ollama.chat(
                model="llama3.2:1b",  # 替换为你安装的模型名称
                messages=[
                    {
                        "role": "user",
                        "content": f"将以下中文短语转换为适合搜索的英文关键词（简洁且相关）：{chinese_input}"
                    }
                ]
            )
            keywords = response["message"]["content"].strip()
            logging.info(f"Translated '{chinese_input}' to '{keywords}'")
            return keywords
        except Exception as e:
            logging.error(f"Failed to translate '{chinese_input}': {e}")
            return chinese_input  # 如果失败，返回原输入作为 fallback