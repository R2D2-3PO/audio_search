from app import create_app
from app.processor import AudioProcessor
from app.es_client import ESClient
import logging

def main():
    app = create_app()
    processor = AudioProcessor()
    logging.info("Starting file processing...")
    audio_files = processor.process_files()
    logging.info(f"Processed {len(audio_files)} files: {audio_files}")  # 添加详细日志

    logging.info("Generating synonym table...")
    synonym_table = processor.generate_synonym_table(audio_files, top_n=10)
    logging.info(f"Generated synonym table: {synonym_table}")

    es_client = ESClient(synonyms=synonym_table)
    logging.info("Indexing data...")
    es_client.index_data(audio_files)

    logging.info("Starting web server on http://127.0.0.1:5002...")
    app.run(debug=True, host='127.0.0.1', port=5002)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Program failed: {e}", exc_info=True)
        raise