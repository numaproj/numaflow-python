import logging


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

LOGGER.addHandler(stream_handler)
