
build:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo build

run:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo run --bin shell

server:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo run --bin server

client:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo run --bin client

test:
	mkdir -p ./tmp_tests/
	RUST_BACKTRACE=1 DATA_FOLDER=./tmp_tests LOG_MODE=6 cargo test --test mod -- --nocapture --test-threads=1
	rm -rf ./tmp_tests/
