# ═══════════════════════════════════════════════════════════════════════════
#  FlexQL Makefile
#
#  LESSON — Makefile basics:
#    target: prerequisites
#    <TAB>  recipe (shell command to run if target is out of date)
#
#  Variables:
#    CXX      = C++ compiler
#    CXXFLAGS = compiler flags
#    LDFLAGS  = linker flags
#
#  Key flags explained:
#    -std=c++17       Use C++17 standard
#    -Wall -Wextra    Enable all/extra warnings (good practice)
#    -g               Include debug symbols (for gdb)
#    -O2              Optimisation level 2 (good balance speed/debug)
#    -I               Add include path
#    -lpthread        Link POSIX threads library
#    -fsanitize=...   Enable AddressSanitizer in debug builds (catches bugs)
# ═══════════════════════════════════════════════════════════════════════════

CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -g -O2 -I./include
LDFLAGS  := -lpthread

# ── Build directories ────────────────────────────────────────────────────
BUILD_DIR  := build
BIN_DIR    := bin

# ── Source files ──────────────────────────────────────────────────────────
# Client sources
CLIENT_SRCS := src/client/main.cpp \
               src/client/flexql_api.cpp

# Server sources (will grow as we implement each module)
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

# ── Object files (replace src/ with build/, .cpp with .o) ────────────────
CLIENT_OBJS := $(patsubst src/%.cpp, $(BUILD_DIR)/%.o, $(CLIENT_SRCS))
SERVER_OBJS := $(patsubst src/%.cpp, $(BUILD_DIR)/%.o, $(SERVER_SRCS))

# ── Default target: build both binaries ───────────────────────────────────
.PHONY: all
all: $(BIN_DIR)/flexql-client $(BIN_DIR)/flexql-server

# ── Link client binary ────────────────────────────────────────────────────
$(BIN_DIR)/flexql-client: $(CLIENT_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "  LD  $@"

# ── Link server binary ────────────────────────────────────────────────────
$(BIN_DIR)/flexql-server: $(SERVER_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "  LD  $@"

# ── Compile rule: src/foo/bar.cpp → build/foo/bar.o ───────────────────────
# LESSON: The automatic variable $< is the first prerequisite (the .cpp file)
#         $@ is the target (the .o file)
#         We create the directory with mkdir -p before compiling.
$(BUILD_DIR)/%.o: src/%.cpp | $(BUILD_DIR)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@
	@echo "  CXX $<"

# ── Create output directories if they don't exist ────────────────────────
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

# ── clean: remove all build artifacts ─────────────────────────────────────
.PHONY: clean
clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)
	@echo "  Cleaned build artifacts"

# ── Debug build: adds AddressSanitizer for memory error detection ─────────
.PHONY: debug
debug: CXXFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer -O0
debug: LDFLAGS  += -fsanitize=address,undefined
debug: all

# ── Run the server (default port 9000) ───────────────────────────────────
.PHONY: run-server
run-server: $(BIN_DIR)/flexql-server
	$(BIN_DIR)/flexql-server 9000

# ── Run the client (connects to localhost:9000) ───────────────────────────
.PHONY: run-client
run-client: $(BIN_DIR)/flexql-client
	$(BIN_DIR)/flexql-client 127.0.0.1 9000

# ── Help ──────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo "FlexQL Build Targets:"
	@echo "  make           - Build client and server"
	@echo "  make debug     - Build with AddressSanitizer"
	@echo "  make clean     - Remove build artifacts"
	@echo "  make run-server - Start the server on port 9000"
	@echo "  make run-client - Open a client REPL"