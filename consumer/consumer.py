import argparse
import json
import logging
import os
import signal
import sys
import threading

import psycopg2
from kafka import KafkaConsumer


class WebchecksConsumer:
    def __init__(self, kafka_uri, kafka_ssl, database_uri):
        self.log = logging.getLogger("producer")
        self.topic_name = "website_checks"
        self.postgresql_uri = database_uri
        self.kafka_uri = kafka_uri
        self.kafka_ssl = kafka_ssl

        self.sql_conn = None
        self.cursor = None
        self.kafka_consumer = None

        self.exit_event = threading.Event()

        self.webcheck_ids = dict()
        signal.signal(signal.SIGINT, self.quit)
        signal.signal(signal.SIGTERM, self.quit)

    def init_kafka_consumer(self):
        self.kafka_consumer = KafkaConsumer(
            self.topic_name,
            auto_offset_reset="earliest",
            max_poll_records=50,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            bootstrap_servers=self.kafka_uri,
            client_id="CONSUMER_CLIENT_ID",
            group_id="CONSUMER_GROUP_ID",
            security_protocol="SSL",
            ssl_cafile=os.path.join(self.kafka_ssl["pem"]),
            ssl_certfile=os.path.join(self.kafka_ssl["cert"]),
            ssl_keyfile=os.path.join(self.kafka_ssl["key"]),
        )

    def init_cursor(self):
        if self.sql_conn:
            self.sql_conn.close()

        self.sql_conn = psycopg2.connect(self.postgresql_uri)
        self.cursor = self.sql_conn.cursor()

    def quit(self, _signal=None, _frame=None):
        self.log.warning("Stopping...")
        self.exit_event.set()

    def create_tables(self):
        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS webchecks (
            id SERIAL PRIMARY KEY,
            url VARCHAR(255) NOT NULL,
            pattern VARCHAR(255)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS webchecks_unique_idx ON webchecks (
            url,
            pattern
        );
        CREATE INDEX IF NOT EXISTS webchecks_url_idx on webchecks USING BTREE  (
            url
        );
        """
        )

        self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS webchecks_results (
            id SERIAL PRIMARY KEY,
            webcheck_id INTEGER NOT NULL,
            status_code SMALLINT,
            response_time FLOAT,
            pattern_found BOOLEAN,
            created_at TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (webcheck_id) REFERENCES webchecks (id)
        );
        CREATE INDEX IF NOT EXISTS webchecks_results_wcid_idx on webchecks_results USING BTREE  (
            webcheck_id,
            status_code
        );
        """
        )
        self.sql_conn.commit()

    def run(self):
        self.init_cursor()
        self.create_tables()
        self.init_kafka_consumer()

        while not self.exit_event.isSet():
            messages = self.kafka_consumer.poll(timeout_ms=1000)
            for tp, webcheck_records in messages.items():
                for webcheck_record in webcheck_records:
                    webcheck = webcheck_record.value
                    try:
                        self.insert(webcheck)
                    except Exception as ex:
                        self.log.error(f"Failed inserting webcheck {webcheck}", ex)
        self.kafka_consumer.close(2)
        self.sql_conn.close()

    def insert(self, webcheck_result):
        url = webcheck_result["url"]
        pattern = webcheck_result.get("pattern", None)

        webcheck_id = self.webcheck_ids.get((url, pattern), None)
        if not webcheck_id:
            webcheck_id = self.get_webcheck(url, pattern) or self.insert_webcheck(url, pattern)
            self.webcheck_ids[(url, pattern)] = webcheck_id

        self.insert_check_result(webcheck_id, webcheck_result)
        self.sql_conn.commit()

    def get_webcheck(self, url, pattern):
        if pattern:
            self.cursor.execute("SELECT id FROM webchecks WHERE url=%s AND pattern=%s;", (url, pattern))
        else:
            self.cursor.execute("SELECT id FROM webchecks WHERE url=%s AND pattern is NULL;", (url,))

        webcheck_id = self.cursor.fetchone()
        return webcheck_id[0] if webcheck_id else None

    def insert_check_result(self, webcheck_id, webcheck_result):
        self.cursor.execute(
            """
        INSERT INTO webchecks_results (webcheck_id, status_code, response_time, pattern_found)
        VALUES (%s, %s, %s, %s)
        """,
            (
                webcheck_id,
                webcheck_result["status_code"],
                webcheck_result["response_time"],
                webcheck_result.get("pattern_found", None),
            ),
        )

    def insert_webcheck(self, url, pattern):
        self.cursor.execute(
            """
        INSERT INTO webchecks (url, pattern)
        VALUES (%s, %s)
        RETURNING id
        """,
            (url, pattern),
        )
        webcheck_id = self.cursor.fetchone()
        return webcheck_id[0] if webcheck_id else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-uri", help="Postgres Service URI", required=True)
    parser.add_argument("--kafka-uri", help="Kafka Service URI", required=True)
    parser.add_argument("--kafka-ssl", help="Kafka Service security files (.pem, .cert, .key) directory path", required=True)
    args = parser.parse_args()

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

    consumer = WebchecksConsumer(args.kafka_uri, kafka_ssl, args.pg_uri)
    consumer.run()


if __name__ == "__main__":
    sys.exit(main())
