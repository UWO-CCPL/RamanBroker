import datetime
import pytz
from dateutil.tz import tzlocal
import logging
import os
import threading
import csv
import time
from ConfigParser import ConfigParser

from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler, FileSystemEvent
import influxdb
import re

TIMESTAMP_EXTRACTOR = re.compile(r"\\([0-9\- ]*) _Spectrum.csv")


class RamanBrokerNewDataFileHandler(FileSystemEventHandler):
    def __init__(self, config):
        """

        :type config: ConfigParser
        """
        self.config = config
        self.logger = logging.getLogger("RamanBroker")

        host = config.get("influxdb", "host")
        port = config.getint("influxdb", "port")
        username = config.get("influxdb", "username")
        password = config.get("influxdb", "password")
        database = config.get("influxdb", "database")
        self.measurement = config.get("influxdb", "measurement")
        self.monitor_directory = config.get("raman", "monitor_directory")

        self.client = influxdb.InfluxDBClient(host, port, username, password, database)
        self.logger.info("InfluxDB Connected: http://{}:{}.".format(host, port))

    def on_created(self, event):
        """

        :type event: FileSystemEvent
        """
        path = event.src_path
        if not self.filter_target(path):
            return

        self.logger.info("File created: {}".format(path))

        timestamp = TIMESTAMP_EXTRACTOR.search(path)
        timestamp = datetime.datetime.strptime(timestamp.group(1), "%Y-%m-%d %H-%M-%S")
        timestamp = timestamp.replace(tzinfo=tzlocal())
        timestamp = timestamp.astimezone(pytz.UTC)

        # spin lock to check if writing is completed
        count_down = 30
        while count_down >= 0:
            if os.path.getsize(path) > 0:
                break
            self.logger.debug("Waiting file {} for writting: {}".format(path, count_down))
            count_down -= 1
            time.sleep(1)
        if count_down < 0:
            self.logger.error("Failed to wait file {} for writting".format(path))
            return

        fields = {}
        with open(path, 'r') as f:
            reader = csv.reader(f)
            for line in reader:
                try:
                    wave_number = line[0]
                    count = line[1]
                    fields[str(wave_number)] = count
                except:
                    break

        experiment = self.get_experiment(path)
        json_body = [{
            "measurement": self.measurement,
            "tags": dict([("experiment", experiment)]) if experiment is not None else {},
            "time": timestamp.isoformat(),
            "fields": fields
        }]
        self.client.write_points(json_body)
        self.logger.info("Point written to InfluxDB")

    def filter_target(self, filename):
        """

        :type filename: str
        """
        return filename.endswith("csv")

    def get_experiment(self, path):
        dirname = os.path.dirname(path)
        if dirname == self.monitor_directory:
            return None
        else:
            return os.path.basename(dirname)


class RamanBroker(threading.Thread):
    file_observer = None  # type: PollingObserver
    config = None  # type: ConfigParser
    event_handler = None  # type: RamanBrokerNewDataFileHandler

    def __init__(self, config):
        super(RamanBroker, self).__init__()
        self.config = config
        self.logger = logging.getLogger("RamanBroker")

    def run(self):
        self.configure_file_monitor()

        self.file_observer.join()

    def configure_file_monitor(self):
        monitor_directory = self.config.get("raman", "monitor_directory")
        polling_timeout = self.config.getfloat("raman", "polling_timeout_second")

        self.event_handler = RamanBrokerNewDataFileHandler(self.config)
        self.file_observer = PollingObserver(polling_timeout)
        self.file_observer.schedule(self.event_handler, monitor_directory, recursive=True)
        self.file_observer.start()

        self.logger.info(
            "File watchdog started monitoring {} with polling timeout {} seconds.".format(monitor_directory,
                                                                                          polling_timeout))

    def stop(self):
        try:
            self.file_observer.stop()
        except Exception as ex:
            self.logger.error(ex.message)

        try:
            self.event_handler.client.close()
        except Exception as ex:
            self.logger.error(ex.message)
