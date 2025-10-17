# Peer-to-Peer File Sharing System

## 1. System Description

A distributed P2P file sharing system implementing BitTorrent-like functionality with centralized tracking. The system enables efficient parallel downloads using chunk-based transfers, rarest-first strategy, and integrity verification.

### Architecture
- **Tracker**: Central coordinator managing peer registry, file metadata, and chunk ownership.
- **Peer**: Peer registers to the P2P network and also serves chunks to other peers based on chunk availability.
- **Protocol**: JSON over length-prefixed TCP (4-byte big-endian length header + JSON payload)

### Key Features
1. **Chunk-based Transfer**: Files split into 512KB chunks for parallel downloads
2. **Integrity Verification**: SHA-256 hashing for each chunk, validated against authoritative hashes
3. **Rarest-first Strategy**: Prioritizes downloading scarce chunks to improve swarm health
4. **RTT-based Peer Selection**: Measures round-trip time and selects fastest peers
5. **Dynamic Peer Discovery**: Refreshes peer list every second during downloads
6. **Immediate Seeding**: Downloaded chunks become available to other peers instantly
7. **Parallel Workers**: 10 concurrent download workers for maximum throughput

## 2. Protocol Specifications

### Message Format
```
[4-byte length (big-endian)] [JSON payload]
```

### Tracker Messages

#### REQUEST_REGISTER
```json
{
  "type": "REQUEST_REGISTER",
  "address": ["<ip>", <port>]
}
```
Response: `REPLY_REGISTER`

#### REQUEST_PUBLISH
```json
{
  "type": "REQUEST_PUBLISH",
  "filename": "file.txt",
  "fileinfo": {
    "size": 1024000,
    "total_chunknum": 2
  },
  "org_hashes": ["<sha256>", "<sha256>", ...]
}
```
Response: `REPLY_PUBLISH` with `result: true/false`

#### REQUEST_FILE_LIST
```json
{
  "type": "REQUEST_FILE_LIST"
}
```
Response: `REPLY_FILE_LIST` with `file_list` dict

#### REQUEST_FILE_LOCATION
```json
{
  "type": "REQUEST_FILE_LOCATION",
  "filename": "file.txt"
}
```
Response: `REPLY_FILE_LOCATION` with `fileinfo` and `chunkinfo`

#### REQUEST_CHUNK_REGISTER
```json
{
  "type": "REQUEST_CHUNK_REGISTER",
  "filename": "file.txt",
  "chunknum": 5
}
```
No response required.

### Peer-to-Peer Messages

#### PEER_REQUEST_CHUNK
```json
{
  "type": "PEER_REQUEST_CHUNK",
  "filename": "file.txt",
  "chunknum": 3
}
```

#### PEER_REPLY_CHUNK
```json
{
  "type": "PEER_REPLY_CHUNK",
  "filename": "file.txt",
  "chunknum": 3,
  "data": [byte_array],
  "digest": "<sha256>"
}
```

#### PEER_PING_PONG
```json
{
  "type": "PEER_PING_PONG"
}
```
Response: Echo same message (used for RTT measurement)

## 3. Program Structure

### Files
- **`main.py`**: Entry point with CLI for tracker and peer modes
- **`tracker.py`**: Tracker server implementation
- **`peer.py`**: Peer client and server implementation
- **`ChunkDownload.log`**: Detailed download log with RTT, chunk sources, and integrity violations.

### Classes

#### `Tracker` (tracker.py)
- **State**: 
  - `_peers`: Maps connections to peer addresses
  - `fileList`: File metadata with org_hashes
  - `chunkInfo`: Chunk ownership per file per peer
- **Methods**:
  - `start(host, port)`: Start tracker server
  - `handle_conn(reader, writer)`: Handle peer connections
  - `stop()`: Cleanup and shutdown

#### `PeerServer` (peer.py)
- **Purpose**: Handles incoming chunk requests from other peers
- **State**: `_files` dict mapping filenames to local paths
- **Methods**:
  - `handle(reader, writer)`: Process PEER_REQUEST_CHUNK and PEER_PING_PONG
  - `add_file(filename, path)`: Register file for serving

#### `PeerClient` (peer.py)
- **Purpose**: Connects to tracker and manages downloads
- **State**:
  - `tracker_reader/writer`: Connection to tracker
  - `handler`: PeerServer instance
  - `peer_id`: Unique 8-char identifier
- **Methods**:
  - `connect_tracker(host, port)`: Register with tracker
  - `publish(abs_path)`: Calculate hashes and publish file
  - `list_files()`: Query available files
  - `download(filename, dest_path)`: Parallel download with integrity checks
  - `measure_peer_rtt(addr)`: Measure round-trip time to peer

### Download Algorithm
1. Query tracker for file metadata and chunk ownership
2. Measure RTT to all peers
3. Spawn 10 worker tasks
4. Each worker works parallely in the following way:
   - Calculate rarity of remaining chunks
   - Select rarest chunk randomly from all the minimum rarest chunk.
   - Choose peer which has the rarest chunk: sort by RTT, pick randomly from top 3.
   - Request chunk, verify SHA-256 against org_hash.
   - If corrupted: mark peer as failed for that chunk, retry with other peers.
   - If valid: write to file, register with tracker ready to serve the chunk.
5. Refresh peer list every 1 second
6. On completion: truncate to actual size, rename temp file.

## 4. What Works

✅ **Core Functionality**
- Tracker registration and peer discovery
- File publishing with SHA-256 hash calculation
- Chunk-based parallel downloads (10 workers)
- Immediate seeding of downloaded chunks

✅ **Advanced Features**
- Rarest-first chunk selection
- RTT measurement and intelligent peer selection
- SHA-256 integrity verification against authoritative hashes
- Corruption detection with automatic retry from different peers
- Dynamic peer discovery during downloads
- Progress bar with real-time speed display

✅ **Robustness**
- Handles peer disconnections gracefully
- Retries failed chunks with different peers
- Fatal error on complete chunk corruption (all peers fail)
- Proper cleanup on exit

## 6. Compilation/Execution

### Prerequisites
- Python 3.7+ (uses asyncio, type hints)
- No external dependencies (stdlib only)

### Running

#### Start Tracker
```bash
python main.py tracker
Tracker> start 0.0.0.0 9100
```

#### Start Peer (Seeder)
```bash
python main.py peer
Peer> connect <tracker_ip> 9100
Peer> publish /absolute/path/to/file.txt
```

#### Start Peer (Downloader)
```bash
python main.py peer
Peer> connect <tracker_ip> 9100
Peer> list_files
Peer> download file.txt /absolute/path/to/save/file.txt
```

### Commands

**Tracker:**
- `start <host> <port>` - Start tracker server
- `list_files` - Show all published files
- `list_peers` - Show connected peers
- `list_chunkinfo` - Show chunk ownership
- `exit` - Shutdown tracker

**Peer:**
- `connect <ip> <port> [peer_host]` - Connect to tracker
- `publish <abs_path>` - Publish file to network
- `list_files` - List available files
- `download <filename> <abs_dest>` - Download file
- `exit` - Disconnect and shutdown

## 9. Design Decisions

1. **512KB Chunks**: Balance between overhead and parallelism
2. **10 Workers**: Optimal for local testing, adjustable for WAN
3. **Rarest-first**: Ensures swarm health, prevents bottlenecks
4. **SHA-256**: Industry-standard integrity verification
5. **Immediate Seeding**: Improves download speeds for later joiners
6. **JSON Protocol**: Human-readable for debugging
7. **asyncio**: Single-threaded concurrency, efficient I/O
