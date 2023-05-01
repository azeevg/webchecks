import unittest
from unittest.mock import patch, MagicMock

import requests
from requests import RequestException

from producer.producer import WebchecksProducer, check_website

CONFIG_PATH = "./config.json"
KAFKA_SSL_PATH = "./ssl"
PG_URI = "pg:1234"
KAFKA_URI = "kafka:1234"

config = {
    "timeout": 1000,
    "period": 3000,
    "websites": [
        {"url": "https://google.com", "timeout": 3000},
        {"url": "https://yahoo.com", "timeout": 500},
        {"url": "https://en.wikipedia.org/wiki/Main_Page", "pattern": "Welcome_to_Wikipedia", "period": 2000},
    ],
}


class ProducerTest(unittest.TestCase):
    def test_webchecks_schedule(self):
        schedules = WebchecksProducer.prepare_webchecks_schedule(config)

        schedules_with_period_3000 = schedules[3000]
        self.assertEqual(len(schedules_with_period_3000), 2)

        schedules_with_period_2000 = schedules[2000]
        self.assertEqual(len(schedules_with_period_2000), 1)

        self.assertEqual(schedules_with_period_3000[0], {"url": "https://google.com", "timeout": 3000})
        self.assertEqual(schedules_with_period_3000[1], {"url": "https://yahoo.com", "timeout": 500})

        self.assertEqual(
            schedules_with_period_2000[0],
            {"url": "https://en.wikipedia.org/wiki/Main_Page", "timeout": 1000, "pattern": "Welcome_to_Wikipedia"},
        )

    @patch("producer.producer.requests")
    def test_successful_check(self, mock_requests, result_handler=MagicMock(), error_handler=MagicMock()):
        mock_requests.get.return_value = MagicMock(status_code=200)

        check_website({"url": "https://google.com", "timeout": 200}, result_handler, error_handler)

        check_result = result_handler.call_args[0][0]

        self.assertEqual(check_result["url"], "https://google.com")
        self.assertEqual(check_result["status_code"], 200)

    @patch("producer.producer.requests")
    def test_pattern_found(self, mock_requests, result_handler=MagicMock(), error_handler=MagicMock()):
        mock_requests.get.return_value = MagicMock(status_code=200, text="This is an html text for pattern search")

        check_website({"url": "https://google.com", "timeout": 200, "pattern": "\\ssearch$"}, result_handler, error_handler)

        check_result = result_handler.call_args[0][0]

        self.assertEqual(check_result["url"], "https://google.com")
        self.assertEqual(check_result["status_code"], 200)
        self.assertEqual(check_result["pattern_found"], True)
        self.assertEqual(check_result["pattern"], "\\ssearch$")

    @patch("producer.producer.requests.get")
    def test_failed_request(self, mock_requests, result_handler=MagicMock(), error_handler=MagicMock()):
        mock_requests.side_effect = requests.exceptions.RequestException

        check_website({"url": "https://google.com", "timeout": 200, "pattern": "\\ssearch$"}, result_handler, error_handler)

        error = error_handler.call_args[0]

        self.assertEqual(error[0], "https://google.com")
        self.assertIsInstance(error[1], RequestException)


if __name__ == "__main__":
    unittest.main()
