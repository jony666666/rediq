import logging
from typing import Literal, Optional

_ROOT_LOGGER_NAME = 'rediq'

def config_logger(
    level: Literal['INFO', 'DEBUG', 'ERROR', 'CRITICAL', 'WARN', 'WARNING'] = 'INFO',
    prefix: Optional[str] = None,
    path: Optional[str] = None,
    echo: bool = False,
):

    logger = logging.getLogger(_ROOT_LOGGER_NAME)
    logger.setLevel(level)
    logger.addHandler(logging.NullHandler())

    prefix_str = f'[{prefix}]' if prefix else ''

    if echo:
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt= f'%(process)d - %(thread)d - %(name)s - %(levelname)s - { prefix_str } %(message)s',
        )
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if path:
        file_handler = logging.FileHandler(path)
        formatter = logging.Formatter(
            fmt= f'%(asctime)s - %(process)d - %(thread)d - %(name)s - %(levelname)s - { prefix_str } %(message)s',
            datefmt='%Y-%m-%d %I:%M:%S',
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

def get_logger(
    name: Optional[str] = None,
):

    root_logger = logging.getLogger(_ROOT_LOGGER_NAME)
    return root_logger.getChild(name) if name else root_logger


