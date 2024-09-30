run:
	DATA_FOLDER=./rusticodb_data cargo run

test:
	mkdir -p ./tmp_tests/
	DATA_FOLDER=./tmp_tests cargo test -- --nocapture --test-threads=1
	rm -rf ./tmp_tests/
