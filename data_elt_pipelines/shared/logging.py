import logging
from datetime import datetime


class Logger:
    def __init__(self):
        self.logger = logging.getLogger()
        # default log level is INFO
        self.logger.setLevel(logging.INFO)

        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        log_filename = f"/var/log/log-{timestamp}.log"

        self.file_handler = logging.FileHandler(log_filename)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.file_handler.setFormatter(formatter)
        self.logger.addHandler(self.file_handler)

    @property
    def name(self):
        return self.logger.name

    @name.setter
    def name(self, name: str):
        self.logger.name = name

    @property
    def level(self):
        return self.logger.level

    @level.setter
    def level(self, level: int):
        self.logger.setLevel(level)

    def log(self, message: str, log_level: int = logging.INFO):
        if log_level == logging.DEBUG:
            self.logger.debug(message)
        elif log_level == logging.INFO:
            self.logger.info(message)
        elif log_level == logging.WARNING:
            self.logger.warning(message)
        elif log_level == logging.ERROR:
            self.logger.error(message)
        elif log_level == logging.CRITICAL:
            self.logger.critical(message)
        else:
            self.logger.info(message)

    def close(self):
        self.file_handler.close()
        self.logger.removeHandler(self.file_handler)


logger = Logger()
