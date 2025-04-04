from app import create_app
from app.processor import AudioProcessor
from app.es_client import ESClient
import logging

def main():
    app = create_app()
    processor = AudioProcessor()

    try:
        audio_files = processor.process_files()
        # 生成动态近义词表
        synonym_table = processor.generate_synonym_table(audio_files, top_n=10)
        # 使用动态词表创建索引
        es_client = ESClient(synonyms=synonym_table)
        es_client.index_data(audio_files)
        logging.info("Starting web server on http://127.0.0.1:5002...")
        app.run(debug=True, host='127.0.0.1', port=5002)
    except Exception as e:
        logging.error(f"Main process failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()