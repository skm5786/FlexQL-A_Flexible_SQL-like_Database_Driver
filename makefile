# ═══════════════════════════════════════════════════════════════════════════
#  FlexQL Makefile  (perf-optimized build)
#
#  Key changes vs original:
#    -O3           : Full optimizations (vs -O2).  Enables auto-vectorisation,
#                    loop unrolling, and inlining across translation units.
#    -march=native : Use all CPU instructions available on this machine
#                    (AVX2, SSE4.2 etc).  Gives ~5-15% throughput on string
#                    ops (memcpy, strcmp) which dominate INSERT parsing.
#    -flto         : Link-time optimisation.  Allows the linker to inline
#                    cross-file calls (e.g. arena_alloc inlined into
#                    row_insert).  Requires both CXXFLAGS and LDFLAGS.
#    -fno-plt      : Skip the PLT for shared library calls — minor but free.
#    -pipe         : Use pipes instead of temp files between compiler stages.
#
#  The debug target still uses -O0 + ASan so correctness testing is unchanged.
# ═══════════════════════════════════════════════════════════════════════════

CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -I./include \
            -O3 -march=native -flto -fno-plt -pipe
LDFLAGS  := -lpthread -flto

BUILD_DIR  := build
BIN_DIR    := bin

CLIENT_SRCS := src/client/main.cpp \
               src/client/flexql_api.cpp

SERVER_SRCS := src/server/server.cpp \
               src/parser/lexer.cpp \
               src/parser/parser.cpp \
               src/storage/dbmanager.cpp \
               src/storage/storage.cpp \
               src/storage/wal.cpp \
               src/storage/arena.cpp \
               src/index/index.cpp \
               src/index/btree.cpp \
               src/cache/cache.cpp \
               src/expiration/expiration.cpp \
               src/network/network.cpp

CLIENT_OBJS := $(patsubst src/%.cpp, $(BUILD_DIR)/%.o, $(CLIENT_SRCS))
SERVER_OBJS := $(patsubst src/%.cpp, $(BUILD_DIR)/%.o, $(SERVER_SRCS))

.PHONY: all
all: $(BIN_DIR)/flexql-client $(BIN_DIR)/flexql-server

$(BIN_DIR)/flexql-client: $(CLIENT_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "  LD  $@"

$(BIN_DIR)/flexql-server: $(SERVER_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "  LD  $@"

$(BUILD_DIR)/%.o: src/%.cpp | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@
	@echo "  CXX $<"

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)
	@echo "  Cleaned build artifacts"

# Debug: -O0 + AddressSanitizer (no LTO, no march=native for reproducibility)
.PHONY: debug
debug: CXXFLAGS := -std=c++17 -Wall -Wextra -I./include \
                   -O0 -g -fsanitize=address,undefined \
                   -fno-omit-frame-pointer
debug: LDFLAGS  := -lpthread -fsanitize=address,undefined
debug: all

.PHONY: run-server
run-server: $(BIN_DIR)/flexql-server
	$(BIN_DIR)/flexql-server 9000

.PHONY: run-client
run-client: $(BIN_DIR)/flexql-client
	$(BIN_DIR)/flexql-client 127.0.0.1 9000

# ── Benchmark targets ────────────────────────────────────────────────────
# Compiles benchmark_flexql.cpp against the optimised flexql.cpp client lib.
# Uses the same aggressive flags as the server build.

.PHONY: benchmark
benchmark: $(BIN_DIR)/flexql-server
	$(CXX) $(CXXFLAGS) -I./include \
	    benchmark_flexql.cpp src/client/flexql.cpp \
	    -o benchmark $(LDFLAGS)
	@echo ""
	@echo "Built: ./benchmark"
	@echo "Usage:"
	@echo "  ./benchmark --unit-test          # correctness tests only"
	@echo "  ./benchmark 1000000              # 1M row insert benchmark"
	@echo "  ./benchmark 10000000             # 10M row insert benchmark"

.PHONY: help
help:
	@echo "FlexQL Build Targets:"
	@echo "  make              - Build client and server (optimised)"
	@echo "  make benchmark    - Also compile ./benchmark binary"
	@echo "  make debug        - Build with AddressSanitizer"
	@echo "  make clean        - Remove build artifacts"
	@echo "  make run-server   - Start the server on port 9000"
	@echo "  make run-client   - Open a client REPL"