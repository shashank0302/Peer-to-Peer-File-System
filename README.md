# Peer-to-Peer Network File System

A P2P file sharing system with tracker and peer nodes.

## Quick Start

### Start Tracker
```bash
python main.py tracker
Tracker> start 127.0.0.1 9100
```

### Start Peer (Seeder)
```bash
python main.py peer
Peer> connect 127.0.0.1 9100
Peer> publish /absolute/path/to/file.ext
```

### Start Peer (Downloader)
```bash
python main.py peer
Peer> connect 127.0.0.1 9100
Peer> list_files
Peer> download file.ext /absolute/path/to/save/file.ext
```

## Commands

### Tracker Commands
- `start <host> <port>` - Start tracker server
- `list_files` - Show available files
- `list_peers` - Show connected peers
- `list_chunkinfo` - Show chunk ownership
- `exit` - Stop tracker

### Peer Commands
- `connect <ip> <port>` - Connect to tracker
- `publish <abs_path>` - Share a file
- `list_files` - List available files
- `download <file> <abs_dest>` - Download file
- `exit` - Stop peer

## Features
- JSON protocol over length-prefixed TCP
- Chunk-based file transfer (512KB chunks)
- Rarest-first download strategy
- RTT-based peer selection
- Automatic chunk registration

## Testing
1. Start tracker on port 9100
2. Start peer A, connect, publish a file
3. Start peer B, connect, download the file
4. Check tracker shows both peers and chunk info
