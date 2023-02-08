import logging
import os

if os.getenv("PYTHONDEBUG"):
    os.environ["PYTHONASYNCIODEBUG"] = "1"


def setup_logging(name):
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(stream_handler)
    return logger
