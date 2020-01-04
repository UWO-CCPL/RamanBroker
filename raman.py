import datetime
import json
import struct

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
from paho.mqtt.client import Client

TIMESTAMP_EXTRACTOR = re.compile(r"\\([0-9\- ]*) _Spectrum.csv")


class RamanBrokerNewDataFileHandler(FileSystemEventHandler):
    def __init__(self, config):
        """

        :type config: ConfigParser
        """
        self.wave_number_transmitted = False
        self.config = config
        self.logger = logging.getLogger("RamanBroker")

        self.influxdb_enabled = config.getboolean("influxdb", "enabled")
        if self.influxdb_enabled:
            host = config.get("influxdb", "host")
            port = config.getint("influxdb", "port")
            username = config.get("influxdb", "username")
            password = config.get("influxdb", "password")
            database = config.get("influxdb", "database")
            self.measurement = config.get("influxdb", "measurement")
            self.monitor_directory = config.get("raman", "monitor_directory")
            self.influx_client = influxdb.InfluxDBClient(host, port, username, password, database)
            self.logger.info("InfluxDB Connected: http://{}:{}.".format(host, port))

        self.mqtt_enabled = config.getboolean("mqtt", "enabled")
        if self.mqtt_enabled:
            mqtt_id = config.getboolean("mqtt", "id")
            mqtt_host = config.getboolean("mqtt", "host")
            mqtt_port = config.getboolean("mqtt", "port")
            self.mqtt_client = Client(mqtt_id)
            self.mqtt_client.connect(mqtt_host, mqtt_port)
            self.logger.info("MQTT Connected: tcp://{}:{}.".format(mqtt_host, mqtt_port))

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
        wave_numbers = []
        counts = []
        with open(path, 'r') as f:
            reader = csv.reader(f)
            for line in reader:
                try:
                    wave_number = line[0]
                    count = line[1]
                    fields[str(wave_number)] = count
                    wave_numbers.append(wave_number)
                    counts.append(count)
                except:
                    break

        if self.influxdb_enabled:
            experiment = self.get_experiment(path)
            json_body = [{
                "measurement": self.measurement,
                "tags": dict([("experiment", experiment)]) if experiment is not None else {},
                "time": timestamp.isoformat(),
                "fields": fields
            }]
            self.influx_client.write_points(json_body)
            self.logger.info("Point written to InfluxDB")

        if self.mqtt_enabled:
            topic = self.config.get("mqtt", "topic")
            counts_topic = os.path.join(topic, "count")

            payload = json.dumps(counts)
            self.mqtt_client.publish(counts_topic, payload, qos=0)
            self.logger.info("Points written to MQTT")
            if not self.wave_number_transmitted:
                wave_number_topic = os.path.join(topic, "wave_number")
                wave_number_payload = json.dumps(wave_numbers)
                self.mqtt_client.publish(wave_number_topic, wave_number_payload, qos=2, retain=True)
                self.wave_number_transmitted = True
                self.logger.info("Wave number has been written to {}".format(wave_number_topic))

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
            self.event_handler.influx_client.close()
        except Exception as ex:
            self.logger.error(ex.message)
