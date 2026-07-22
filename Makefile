CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -pthread -Iinclude
BIN      = bin

all: $(BIN)/store_node $(BIN)/sync_node $(BIN)/client

$(BIN):
	mkdir -p $(BIN)

$(BIN)/store_node: src/store_node.cpp include/common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) src/store_node.cpp -o $@

$(BIN)/sync_node: src/sync_node.cpp include/common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) src/sync_node.cpp -o $@

$(BIN)/client: src/client.cpp include/common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) src/client.cpp -o $@

clean:
	rm -rf $(BIN) logs/*.log

.PHONY: all clean
