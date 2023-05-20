import logging


def get_logger(name: str):
    """Returns an instance of the logger"""

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)-8s  %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
