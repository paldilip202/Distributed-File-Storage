# Distributed File Storage System

A distributed file storage system built from scratch in **C++17**, inspired by the architecture of **Google File System (GFS)** and **HDFS**. Implements chunk-based storage, replication, automatic failure detection, and self-healing — all over raw TCP sockets with no external libraries.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        CLIENT                           │
│         splits files · stores chunks · fetches files   │
└───────────────────────┬─────────────────────────────────┘
                        │  TCP (port 8080)
                        ▼
┌─────────────────────────────────────────────────────────┐
│                     MASTER NODE                         │
│   metadata · chunk locations · failure detection ·     │
│   heartbeat monitoring · re-replication orchestration  │
└────────────┬──────────────────────────┬────────────────┘
             │ TCP                      │ TCP
             ▼                          ▼
┌────────────────────┐      ┌────────────────────┐
│   CHUNK SERVER 1   │      │   CHUNK SERVER 2   │
│   port 9001        │◄────►│   port 9002        │
│   stores chunks    │      │   stores chunks    │
│   sends heartbeats │      │   sends heartbeats │
└────────────────────┘      └────────────────────┘
```

**Store flow:** Client splits file → asks Master for servers → sends chunks to N servers simultaneously → reports locations to Master → Master persists metadata to disk.

**Fetch flow:** Client asks Master for chunk locations → receives all replica addresses per chunk → fetches each chunk with automatic fallback if a server is unreachable → reassembles original file locally.

---

## Features

- **Chunk-based storage** — files split into 1MB chunks stored as independent binary files
- **Replication** — every chunk stored on 2 servers simultaneously (configurable)
- **Automatic failure detection** — heartbeat-based monitoring with configurable timeout
- **Self-healing** — Master automatically re-replicates orphaned chunks when a server dies
- **Fault-tolerant reads** — client tries replica servers automatically if primary is unreachable
- **Metadata persistence** — metadata saved to disk so the Master survives restarts
- **Round-robin load balancing** — chunk distribution spread evenly across all servers
- **Peer-to-peer re-replication** — chunk servers transfer data directly without routing through Master
- **Thread-safe design** — mutex-protected shared state throughout

---

## Tech Stack

| Area | Technology |
|---|---|
| Language | C++17 |
| Networking | POSIX TCP sockets (`sys/socket.h`) |
| Concurrency | `std::thread`, `std::mutex`, `std::lock_guard` |
| Storage | Binary file I/O (`fstream`) |
| Filesystem | `std::filesystem` (C++17) |
| Protocol | Custom newline-delimited text protocol |
| Build | GNU Make |
| Platform | Linux / WSL2 |

---

## Project Structure

```
distributed-file-system/
├── src/
│   ├── metadata.h          # ChunkInfo, FileMetadata, MetadataStore declarations
│   ├── metadata.cpp        # Metadata store — in-memory + disk persistence
│   ├── chunker.h           # File splitting/assembly declarations
│   ├── chunker.cpp         # File chunking and reassembly logic
│   ├── master.cpp          # Master Node — metadata, heartbeat monitor, re-replication
│   ├── chunkserver.cpp     # Chunk Server — stores/serves chunks, sends heartbeats
│   └── client.cpp          # Client — store and fetch files over the network
└── Makefile
```

---

## Getting Started

### Prerequisites

- Linux or WSL2 (Ubuntu 20.04+)
- GCC 9+ with C++17 support
- GNU Make

```bash
# Install on Ubuntu/WSL2
sudo apt update
sudo apt install g++ make -y

# Verify
g++ --version   # should be 9.x or higher
```

### Build

```bash
git clone https://github.com/<your-username>/distributed-file-system.git
cd distributed-file-system
make
```

This produces three binaries: `master`, `chunkserver`, and `client`.

---

## Running the System

You need **four terminal windows**, started in this exact order.

**Terminal 1 — Start the Master Node**
```bash
./master
```
```
[Master] Running on port 8080
[Master] Heartbeat timeout: 9s
```

**Terminal 2 — Start Chunk Server 1**
```bash
./chunkserver 9001
```
```
[CS:9001] Registered with Master.
[CS:9001] Running on port 9001
```

**Terminal 3 — Start Chunk Server 2**
```bash
./chunkserver 9002
```
```
[CS:9002] Registered with Master.
[CS:9002] Running on port 9002
```

**Terminal 4 — Use the Client**
```bash
# Create a test file
dd if=/dev/urandom of=testfile.bin bs=1M count=5

# Store it in the distributed system
./client store testfile.bin

# Retrieve it
./client fetch testfile.bin

# Verify byte-perfect reconstruction
diff testfile.bin restored_testfile.bin && echo "PERFECT"
```

---

## Testing Fault Tolerance

### Test 1 — Replica Fallback

After storing a file, kill Chunk Server 1 (`Ctrl+C` in Terminal 2), then fetch:

```bash
./client fetch testfile.bin
```

The client automatically detects the dead server and fetches every chunk from its replica on Server 2:

```
[WARN] Chunk 0: localhost:9001 unreachable, trying next replica...
Fetched chunk 0 <- localhost:9002
Fetched chunk 1 <- localhost:9002
...
File restored -> restored_testfile.bin
```

```bash
diff testfile.bin restored_testfile.bin && echo "SURVIVED SERVER FAILURE"
```

### Test 2 — Automatic Re-replication (Self-healing)

Kill Chunk Server 1 and watch Terminal 1. After 9 seconds the Master automatically detects the failure and re-replicates all orphaned chunks to the surviving server:

```
[Master] FAILURE DETECTED: localhost:9001 (silent for 9s)
[Master] Starting re-replication for chunks lost on: localhost:9001
[Master] Re-replicating testfile.bin_chunk_0: localhost:9002 → localhost:9002
[Master] Restored: testfile.bin_chunk_0
[Master] Re-replication complete. 3 chunk(s) restored.
```

The replication factor is automatically restored to 2 — no human intervention required.

---

## System Design Decisions

### Why chunk-based storage?
Storing files as fixed-size chunks makes it possible to distribute a single large file across many servers. No single server needs to hold an entire file, and chunks from different files can be interleaved across servers for even utilization.

### Why a dedicated Master Node?
The Master holds all metadata in memory, making chunk lookups O(1) and avoiding expensive distributed coordination for every read. This is the same design choice GFS made — a single Master simplifies the system significantly at the cost of a single point of failure (mitigated by metadata persistence to disk).

### Why heartbeats over persistent connections?
Each chunk server opens a fresh TCP connection per heartbeat. This avoids silent connection drops from firewalls or OS keepalive timeouts that would give the Master a false sense of server health.

### Why peer-to-peer re-replication?
When the Master triggers re-replication, it tells one chunk server to transfer data directly to another. The Master never touches the file data itself. This keeps the Master lightweight and avoids it becoming a network bottleneck during recovery.

### Why release locks before re-replication?
The monitor thread collects dead servers inside a short locked section, then releases all mutexes before calling `replicateOrphanedChunks()`. Re-replication involves slow network I/O (seconds), so holding locks during this time would starve every other thread waiting for the chunk server list or heartbeat map.

---

## Protocol Reference

All messages are newline-delimited (`\n`). Raw binary data follows immediately after its command line with no separator.

| Sender | Receiver | Message | Description |
|---|---|---|---|
| Chunk Server | Master | `REGISTER host port` | Announce server on startup |
| Chunk Server | Master | `HEARTBEAT host port` | Periodic liveness signal |
| Client | Master | `STORE filename size` | Begin a file store operation |
| Master | Client | `CS host:port` (×N) + `END` | Available chunk servers |
| Client | Master | `CHUNK_DONE chunk_id N server... index size` | Report stored chunk |
| Client | Master | `FETCH filename` | Request chunk locations |
| Master | Client | `OK N` + chunk lines + `END` | Chunk metadata response |
| Client | Chunk Server | `PUT chunk_id size` + bytes | Store a chunk |
| Client | Chunk Server | `GET chunk_id` | Retrieve a chunk |
| Chunk Server | Chunk Server | `PUT chunk_id size` + bytes | Peer-to-peer re-replication |
| Master | Chunk Server | `REPLICATE chunk_id host port` | Trigger re-replication |

---

## Configuration

All configuration is via constants at the top of each source file:

| Constant | File | Default | Description |
|---|---|---|---|
| `CHUNK_SIZE` | `chunker.h` | 1MB | Size of each file chunk |
| `MASTER_PORT` | `master.cpp` | 8080 | Master Node listening port |
| `REPLICATION_FACTOR` | `master.cpp` | 2 | Number of replicas per chunk |
| `HEARTBEAT_TIMEOUT` | `master.cpp` | 9s | Seconds before declaring server dead |
| `HEARTBEAT_INTERVAL` | `chunkserver.cpp` | 3s | Seconds between heartbeats |

---

## Concepts Demonstrated

This project is a ground-up implementation of concepts taught in distributed systems courses and used in production infrastructure:

- **CAP theorem** — this system prioritises Availability and Partition tolerance over strong Consistency
- **Replication** — write amplification in exchange for read-time fault tolerance
- **Heartbeat pattern** — periodic liveness signals to detect silent failures
- **RAII** — `std::lock_guard` ensures mutexes are always released, even on exceptions
- **Write-ahead persistence** — metadata saved to disk after every chunk operation
- **Round-robin load balancing** — even distribution of primary and replica chunks
- **Peer-to-peer data transfer** — chunk servers communicate directly during recovery

---

## Inspiration

- **Google File System (GFS)** — Ghemawat, Gobioff, Leung (2003)
- **Hadoop Distributed File System (HDFS)** — Shvachko et al. (2010)
- **Designing Data-Intensive Applications** — Martin Kleppmann

---

## License

MIT License — see [LICENSE](LICENSE) for details.