
build:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo build

run:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo run --bin rusticodbshell

server:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo run --bin rusticodbserver

client:
	DATA_FOLDER=./rusticodb_data LOG_MODE=3 cargo run --bin rusticodbclient

test:
	mkdir -p ./tmp_tests/
	RUST_BACKTRACE=1 DATA_FOLDER=./tmp_tests LOG_MODE=6 cargo test -- --nocapture --test-threads=1
	rm -rf ./tmp_tests/
