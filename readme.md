# FlexQL

A lightweight SQL-like database server written from scratch in C++17.
FlexQL runs as a TCP server; clients connect, issue SQL statements, and receive typed results through a clean binary wire protocol.

**GitHub:** https://github.com/skm5786/FlexQL-A_Flexible_SQL-like_Database_Driver

---

## Requirements

- g++ with C++17 support (GCC 9+)
- POSIX threads (`-lpthread`)
- Linux
- GNU Make

No external database libraries are used.

---

## Building

```bash
make          # optimised build (-O3 -march=native)
make debug    # debug build with AddressSanitizer + UBSan
make clean    # remove build artefacts
```

Binaries are placed in `bin/`.

---

## Running

**Start the server** (default port 9000):

```bash
./bin/flexql-server 9000
```

**Open the interactive REPL** in a second terminal:

```bash
./bin/flexql-client 127.0.0.1 9000
```

You should see:

```
Connected to FlexQL server
flexql>
```

---

## Quick Start

```sql
flexql> CREATE DATABASE SCHOOL;

flexql> USE SCHOOL;

flexql> CREATE TABLE STUDENT(ID INT PRIMARY KEY NOT NULL, NAME TEXT NOT NULL);
Table 'STUDENT' created.

flexql> INSERT INTO STUDENT VALUES (1, 'Alice');
1 row inserted.

flexql> INSERT INTO STUDENT VALUES (2, 'Bob');
1 row inserted.

flexql> SELECT * FROM STUDENT;
ID = 1
NAME = Alice

ID = 2
NAME = Bob

flexql> SELECT * FROM STUDENT WHERE ID = 1;
ID = 1
NAME = Alice

flexql> .exit
Connection closed
```

---

## Supported SQL

| Statement | Example |
|---|---|
| `CREATE TABLE` | `CREATE TABLE t (id INT PRIMARY KEY NOT NULL, name TEXT NOT NULL);` |
| `DROP TABLE` | `DROP TABLE t;` |
| `INSERT` | `INSERT INTO t VALUES (1, 'Alice');` |
| `INSERT` (batch) | `INSERT INTO t VALUES (1,'A'),(2,'B'),(3,'C');` |
| `SELECT *` | `SELECT * FROM t;` |
| `SELECT columns` | `SELECT id, name FROM t;` |
| `WHERE` | `SELECT * FROM t WHERE id = 5;` |
| `WHERE` (range) | `SELECT * FROM t WHERE balance > 1000;` |
| `INNER JOIN` | `SELECT * FROM orders INNER JOIN customers ON orders.cid = customers.id;` |
| `CREATE DATABASE` | `CREATE DATABASE mydb;` |
| `USE` | `USE mydb;` |
| `SHOW DATABASES` | `SHOW DATABASES;` |
| `SHOW TABLES` | `SHOW TABLES;` |
| `DROP DATABASE` | `DROP DATABASE mydb;` |

**Column types:** `INT`, `DECIMAL`, `VARCHAR(n)`, `TEXT`, `DATETIME`

**WHERE operators:** `=`, `!=`, `<`, `>`, `<=`, `>=`

**Constraints:** `NOT NULL`, `PRIMARY KEY`

> Only one WHERE condition per query. AND/OR are not supported.

---

## REPL Meta-Commands

| Command | Action |
|---|---|
| `.exit` or `.quit` | Close connection and exit |
| `.help` | Show supported statements |

Multi-line statements are supported — keep typing until you end with `;`.


---

## Benchmarking

### benchmark_flexql (correctness + insert throughput)

`benchmark_flexql.cpp` is the primary test program. It runs correctness unit
tests and measures bulk INSERT throughput.

**Step 1 — compile the benchmark** (server must already be built with `make`):

```bash
g++ -std=c++17 -O3 -march=native -I./include \
    benchmark_flexql.cpp src/client/flexql.cpp \
    -o benchmark -lpthread
```

**Step 2 — start the server** in one terminal:

```bash
./bin/flexql-server 9000
```

**Step 3 — run** in another terminal:

```bash
# Correctness tests only (21 unit tests, ~seconds)
./benchmark --unit-test

# 100K-row insert benchmark (quick smoke test)
./benchmark 100000

# 1M-row insert benchmark
./benchmark 1000000

# 10M-row insert benchmark (full throughput target, ~60-120 seconds)
./benchmark 10000000
```
---

## Features

- **Multithreaded server** — one thread per client, full concurrent access
- **Row-major storage** with a per-table arena allocator (no per-row malloc)
- **Hash index** on primary key columns — O(1) equality lookups
- **B+ tree index** on numeric columns — O(log n + k) range queries
- **LRU query result cache** — repeated SELECT queries served from memory
- **Row expiration** — rows can carry a Unix-timestamp expiry; a background thread purges them periodically
- **Write-ahead log** — append-only WAL with CRC32 and group-commit for crash recovery
- **Multi-database** support via `CREATE DATABASE` / `USE`
- **Batch INSERT** — insert thousands of rows in a single statement
- **Fast INSERT path** — hand-written scanner converts numeric values once, bypassing the full parser for common INSERT patterns

---

## Stopping the Server

Press `Ctrl+C` in the server terminal. The server flushes the WAL buffer,
drops all session databases, and exits cleanly.