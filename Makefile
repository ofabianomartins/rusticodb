run:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo run

test:
	mkdir -p ./tmp_tests/
	DATA_FOLDER=./tmp_tests LOG_MODE=6 cargo test -- --nocapture --test-threads=1
	rm -rf ./tmp_tests/
