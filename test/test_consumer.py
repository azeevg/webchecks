import unittest
from unittest.mock import MagicMock, patch

from consumer.consumer import WebchecksConsumer

KAFKA_SSL_PATH = "./ssl"
PG_URI = "pg:1234"
KAFKA_URI = "kafka:1234"


def _consumer():
    consumer = WebchecksConsumer(KAFKA_URI, KAFKA_SSL_PATH, PG_URI)
    consumer.cursor = MagicMock()
    consumer.sql_conn = MagicMock()

    cached_webcheck_ids = {("http://google.com", ".*google.*"): 100}
    consumer.webcheck_ids = cached_webcheck_ids

    return consumer


class ConsumerTest(unittest.TestCase):
    def test_creates_tables(self, consumer=_consumer()):
        consumer.create_tables()
        self.assertEqual(consumer.cursor.execute.call_count, 2)
        self.assertEqual(consumer.sql_conn.commit.call_count, 1)

    def test_insert_webcheck_result__cached_id(self, consumer: WebchecksConsumer = _consumer()):
        webcheck_result = {
            "url": "http://google.com",
            "status_code": 200,
            "response_time": 32,
            "pattern_found": True,
            "pattern": ".*google.*",
        }

        consumer.insert(webcheck_result)
        args = consumer.cursor.execute.call_args[0][1]
        self.assertEqual(args, (100, 200, 32, True))

    def next_id(self):
        return [101]

    def test_insert_webcheck_result__no_cached_id__no_insertion_needed(self, consumer: WebchecksConsumer = _consumer()):
        webcheck_result = {"url": "http://yahoo.com", "status_code": 200, "response_time": 28}

        with patch.object(consumer.cursor, "fetchone", self.next_id):
            consumer.insert(webcheck_result)

        selection_args = consumer.cursor.execute.call_args_list[0][0][1]
        self.assertEqual(selection_args, ("http://yahoo.com",))

        insertion_args = consumer.cursor.execute.call_args_list[1][0][1]
        self.assertEqual(insertion_args, (101, 200, 28, None))

    def test_insert_webcheck_result__no_cached_id__insertion_needed(self, consumer=_consumer()):
        webcheck_result = {
            "url": "http://mysite123.com",
            "status_code": 404,
            "response_time": 98,
            "pattern": ".*elem.*",
            "pattern_found": False,
        }

        consumer.get_webcheck = MagicMock(return_value=None)  # webcheck is not presented in DB
        with patch.object(consumer.cursor, "fetchone", self.next_id):
            consumer.insert(webcheck_result)

        webchek_insertion_args = consumer.cursor.execute.call_args_list[0][0][1]
        self.assertEqual(webchek_insertion_args, ("http://mysite123.com", ".*elem.*"))

        insertion_args = consumer.cursor.execute.call_args_list[1][0][1]
        self.assertEqual(insertion_args, (101, 404, 98, False))

        self.assertEqual(consumer.sql_conn.commit.call_count, 1)


if __name__ == "__main__":
    unittest.main()
