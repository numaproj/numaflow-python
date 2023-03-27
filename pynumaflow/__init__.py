import logging
import os
import sys

if os.getenv("PYTHONDEBUG"):
    os.environ["PYTHONASYNCIODEBUG"] = "1"


class StdoutFilter(logging.Filter):
    """
    Filter logs with level less than logging.ERROR so they will go to stdout instead
    of default stderr
    """

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.ERROR


def setup_logging(name):
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger(name)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.addFilter(StdoutFilter())
    stdout_handler.setLevel(logging.INFO)
    logger.addHandler(stdout_handler)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    stderr_handler.setLevel(logging.ERROR)
    logger.addHandler(stderr_handler)

    return logger
