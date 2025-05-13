import logging
from logging.handlers import RotatingFileHandler
import os

def get_data_pipeline_logger(log_dir: str = "/home/gamma/workspace/log") -> logging.Logger:
    logger = logging.getLogger("data_pipeline_logger")
    logger.setLevel(logging.DEBUG)

    log_path = os.path.join(log_dir, "data_pipeline.log")
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)

        # File handler
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, "data_pipeline.log"),
            maxBytes=5 * 1024 * 1024,
            backupCount=3
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    #logger.propagate = False

    return logger