PYTHON ?= python3
PYTHON_SOURCE_DIRS = consumer/ producer/ test/


fmt:
	black $(PYTHON_SOURCE_DIRS)

flake:
	flake8 $(PYTHON_SOURCE_DIRS)

producer_start:
	$(PYTHON) ./producer/producer.py --config=${WC_CONFIG_PATH} --kafka-uri=${WC_KAFKA_URI} --kafka-ssl=${WC_KAFKA_SSL_PATH}

consumer_start:
	$(PYTHON) ./consumer/consumer.py --pg-uri=${WC_PG_URI} --kafka-uri=${WC_KAFKA_URI} --kafka-ssl=${WC_KAFKA_SSL_PATH}

unittests:
	$(PYTHON) -m unittest discover test/

