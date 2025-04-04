from ftplib import FTP
import logging
from .config import Config

class FTPClient:
    def __init__(self):
        self.ftp = FTP(Config.FTP_HOST)
        self.ftp.encoding = "gbk"
        self.ftp.login(Config.FTP_USER, Config.FTP_PASS)

    def list_dir(self, dir_path):
        files = []
        dirs = []
        logging.info(f"Scanning directory: {dir_path}")
        try:
            self.ftp.cwd(dir_path)
            items = self.ftp.nlst()
        except Exception as e:
            logging.error(f"Error accessing {dir_path}: {e}")
            return files

        for item in items:
            if item in ('.', '..'):
                continue
            try:
                self.ftp.cwd(f"{dir_path}/{item}".replace("//", "/"))
                dirs.append(item)
                self.ftp.cwd(dir_path)
            except:
                if item.endswith(".wav"):
                    files.append((dir_path, item))

        for sub_dir in dirs:
            sub_path = f"{dir_path}/{sub_dir}".replace("//", "/")
            sub_files = self.list_dir(sub_path)
            files.extend(sub_files)

        logging.info(f"Found {len(files)} .wav files in {dir_path}")
        return files

    def get_metadata(self, full_path):
        try:
            full_path = full_path.replace("//", "/").strip()
            size = self.ftp.size(full_path)
            if size is None:
                logging.warning(f"File not found via ftp.size: {full_path}")
                return None
            modified = self.ftp.voidcmd(f"MDTM {full_path}")[4:].strip()
            return {"size": size, "modified": modified}
        except Exception as e:
            logging.error(f"Failed to get metadata for {full_path}: {e}")
            return None

    def close(self):
        self.ftp.quit()