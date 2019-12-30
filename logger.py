import logging
from ConfigParser import ConfigParser


def _configure_logger(config):
    """

    :type config: ConfigParser
    """
    logging.root.setLevel(logging.DEBUG)
    logger = logging.getLogger("RamanBroker")

    level_mapping = {
        "debug": logging.DEBUG,
        "verbose": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")

    # configure console logger
    enable_console = config.getboolean("logging", "console")
    if enable_console:
        console_level = config.get("logging", "console_level").lower()
        level = level_mapping.get(console_level, logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    # configure file logger
    config_logging_file = config.get("logging", "file")
    if config_logging_file:
        file_level = config.get("logging", "file_level").lower()
        level = level_mapping.get(file_level, logging.DEBUG)
        fh = logging.FileHandler(config_logging_file)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
