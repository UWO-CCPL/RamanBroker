import ConfigParser
import logging
import signal
import time

from logger import _configure_logger
from raman import RamanBroker

if __name__ == '__main__':
    cfg_parser = ConfigParser.ConfigParser()
    cfg_parser.read("config.ini")
    _configure_logger(cfg_parser)

    broker = RamanBroker(cfg_parser)
    broker.start()

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        broker.stop()
        broker.join()
