PY=python3

install:
	$(PY) -m pip install -e broker

run-broker:
	$(PY) -m broker.src.broker

run-producer:
	$(PY) clients/producer/producer.py

run-consumer:
	$(PY) clients/consumer/consumer.py

build-c-client:
	cd clients/c_client && make

demo-c-client:
	./scripts/demo_c_client.sh

test:
	cd broker && pytest -q
