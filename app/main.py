from app import create_app
from app.processor import AudioProcessor
from app.es_client import ESClient
import logging

def main():
    app = create_app()
    processor = AudioProcessor()
    es_client = ESClient()

    try:
        audio_files = processor.process_files()
        es_client.index_data(audio_files)
        logging.info("Starting web server...")
        app.run(debug=True, host='127.0.0.1', port=5001)
    except Exception as e:
        logging.error(f"Main process failed: {e}", exc_info=True)  # 显示完整堆栈

if __name__ == "__main__":
    main()