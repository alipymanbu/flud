test:
	poetry run pytest -vv -m "not stress"

test-integration:
	poetry run pytest -vv -m integration

test-stress:
	poetry run pytest -vv -m stress

test-all:
	poetry run pytest -vv

benchmark-dht:
	poetry run python3 flud/test/dht_benchmark.py \
		--nodes 12 \
		--clients 3 \
		--ops-per-phase 10 \
		--concurrency 5 \
		--warmup-ops 15 \
		--key-count 60
