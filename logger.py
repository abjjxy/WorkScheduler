import logging
from logging.handlers import RotatingFileHandler
import os

# 日志目录
LOG_DIR = "./logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def get_logger(name: str = "scheduler") -> logging.Logger:
    logger = logging.getLogger(name)
    # 防止重复添加handler
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # 2. 文件输出，单个文件最大5MB，保留最多5个备份
    file_handler = RotatingFileHandler(
        filename=os.path.join(LOG_DIR, "scheduler.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


# 全局单例logger，项目直接导入使用
app_logger = get_logger("work-scheduler")