import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path


class LogHandler:

    def __init__(self, dir_name: str, file_name: str, level=logging.INFO) -> None:
        self.file_name = file_name
        self.dir_name = dir_name
        self.level = level

    def create_rotating_log(self):
        """
        Creates a rotating log
        """
        logger = logging.getLogger("Rotating Log")
        logger.setLevel(self.level)
        if not os.path.exists(self.dir_name):
            Path(self.dir_name).mkdir(parents=True, exist_ok=True)
        handler = TimedRotatingFileHandler(filename=f"{self.dir_name}/{self.file_name}", when='D', interval=1,
                                           backupCount=10,
                                           encoding='utf-8', delay=False)
        formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger
