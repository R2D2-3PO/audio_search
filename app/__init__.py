from flask import Flask
from .config import Config
import logging
import os

def create_app():
    app = Flask(__name__)

    # 配置日志
    if not os.path.exists("logs"):
        os.makedirs("logs")
    logging.basicConfig(
        filename=Config.LOG_FILE,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # 注册路由
    from .routes import init_routes
    init_routes(app)

    return app