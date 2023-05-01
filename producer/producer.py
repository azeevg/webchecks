import argparse
import json
import logging
import os
import re
import signal
import sys
import threading
import time

import requests
from kafka import KafkaProducer

DEFAULT_PERIOD = 3000
DEFAULT_TIMEOUT = 1000


def check_website(website_check_param, result_handler, error_handler):
    url = website_check_param["url"]
    pattern = website_check_param.get("pattern")
    timeout = website_check_param["timeout"]

    start_time = time.time()
    try:
        response = requests.get(url, timeout=timeout, allow_redirects=True)
    except requests.exceptions.RequestException as ex:
        error_handler(url, ex)
        return

    end_time = time.time()
    response_time = int((end_time - start_time) * 1000)

    check_result = {"url": url, "status_code": response.status_code, "response_time": response_time}

    if pattern:
        check_result["pattern"] = pattern
        check_result["pattern_found"] = bool(re.search(pattern, response.text or ""))

    result_handler(check_result)


class PeriodicExecutor:
    def __init__(self, exit_event: threading.Event, period, jobs, success_handler, error_handler):
        self.error_handler = error_handler
        self.success_handler = success_handler
        self.period = period / 1000.0  # millis to seconds
        self.jobs = jobs
        self.exit_event = exit_event

    def start(self):
        while not self.exit_event.isSet():
            self.run_jobs()
            time.sleep(self.period)

    def run_jobs(self):
        for job in self.jobs:
            args = (job, self.success_handler, self.error_handler)
            threading.Thread(target=check_website, args=args).start()


class WebchecksProducer:
    def __init__(self, config_path, kafka_uri, kafka_ssl):
        self.log = logging.getLogger("producer")
        self.config_path = config_path
        self.config = {}
        self.topic_name = "website_checks"
        self.load_config()
        webchecks_schedule = self.prepare_webchecks_schedule(self.config)
        self.exit_event = threading.Event()
        self.executors = self.create_executors(webchecks_schedule)
        self.kafka_uri = kafka_uri
        self.kafka_ssl = kafka_ssl
        self.producer = None

        signal.signal(signal.SIGINT, self.quit)
        signal.signal(signal.SIGTERM, self.quit)

    def load_config(self):
        try:
            with open(self.config_path) as fp:
                self.config = json.load(fp)
        except Exception:
            self.log.exception("Invalid JSON config, exiting")
            sys.exit(1)

    def init_kafka(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_uri,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            security_protocol="SSL",
            ssl_cafile=self.kafka_ssl["pem"],
            ssl_certfile=self.kafka_ssl["cert"],
            ssl_keyfile=self.kafka_ssl["key"],
        )

    def quit(self, _signal=None, _frame=None):
        self.log.warning("Stopping...")
        self.exit_event.set()

    @staticmethod
    def prepare_webchecks_schedule(config):
        default_period = config.get("period", DEFAULT_PERIOD)
        default_timeout = config.get("timeout", DEFAULT_TIMEOUT)
        websites_conf = config.get("websites")
        webchecks = {}
        for wc in websites_conf:
            period = wc.get("period", default_period)
            timeout = wc.get("timeout", default_timeout)
            if period not in webchecks:
                webchecks[period] = list()

            webcheck_info = {"timeout": timeout, "url": wc["url"]}

            if wc.get("pattern"):
                webcheck_info["pattern"] = wc.get("pattern")

            webchecks[period].append(webcheck_info)
        return webchecks

    def start(self):
        for executor in self.executors:
            executor.start()
        self.log.debug(f"Producer started with {len(self.executors)} executors")

    def create_executors(self, webchecks_schedule):
        executors = []
        for period, webchecks_list in webchecks_schedule.items():
            executors.append(PeriodicExecutor(self.exit_event, period, webchecks_list, self.send_event, self.log_error))
        return executors

    def send_event(self, check_result):
        self.producer.send(self.topic_name, value=check_result)

    def log_error(self, url, ex):
        self.log.error(url, ex)

    def run(self):
        self.init_kafka()
        self.start()
        self.exit_event.wait()
        self.producer.flush()
        self.producer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-uri", help="Kafka Service URI", required=True)
    parser.add_argument("--kafka-ssl", help="Kafka Service security files (.pem, .cert, .key) directory path", required=True)
    parser.add_argument("--config", help="Webchecks configuration file path", required=True)
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"WebsiteChecker: {args.config!r} doesn't exist")
        return 1

    kafka_ssl = {
        "pem": os.path.join(args.kafka_ssl, "ca.pem"),
        "cert": os.path.join(args.kafka_ssl, "service.cert"),
        "key": os.path.join(args.kafka_ssl, "service.key"),
    }

    if not os.path.exists(kafka_ssl["pem"]):
        print(f"WebsiteChecker: {args.kafka_ssl!r} doesn't contain ca.pem.")
        return 1
    if not os.path.exists(kafka_ssl["cert"]):
        print(f"WebsiteChecker: {args.kafka_ssl!r} doesn't contain service.cert")
        return 1
    if not os.path.exists(kafka_ssl["key"]):
        print(f"WebsiteChecker: {args.kafka_ssl!r} doesn't contain service.key")
        return 1

    website_checker = WebchecksProducer(args.config, args.kafka_uri, kafka_ssl)
    website_checker.run()


if __name__ == "__main__":
    sys.exit(main())
