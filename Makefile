.PHONY: all build test benchmark clean

BUILD_DIR := build

all: build

build:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD_DIR) --parallel

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure

benchmark: build
	python3 tools/benchmark.py --server $(BUILD_DIR)/chatApp
	python3 tools/benchmark.py --server $(BUILD_DIR)/chatApp --latency

clean:
	cmake -E rm -rf $(BUILD_DIR)
