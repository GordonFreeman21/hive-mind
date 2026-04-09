#  HIVE-MIND

*A Python 3 AsyncIO BitTorrent client built from scratch.*

Hive-Mind is an educational implementation of the **BitTorrent protocol**. The goal is to build a high-performance backend that communicates via WebSockets to a reactive frontend, visualizing the "swarm" of peers using SVG animations.

---

##  Current Status  ✅ COMPLETE - Download Only Mode

The client is now fully functional for **downloading torrents WITHOUT uploading** (seeding disabled). It implements the complete BitTorrent protocol message loop while refusing all incoming piece requests from peers.

### Key Features
- **Download-only mode**: This client will NEVER upload data to peers
- **Full protocol implementation**: Handshake, bitfield exchange, piece requests, and data assembly
- **Hash verification**: All downloaded pieces are verified against SHA1 hashes
- **AsyncIO networking**: Handles multiple concurrent peer connections efficiently

---

##  Features

###  Implemented
- **Custom B-encoding decoder**  Hand-written parser for the BitTorrent data format.
- **Smart tracker client**  Supports HTTP/HTTPS multi-tracker failover (handles announce-list and 503 errors).
- **AsyncIO networking**  Uses non-blocking sockets with asyncio.gather and Semaphores to handle high concurrency.
- **The handshake**  Implements the binary handshake protocol (`<pstrlen><pstr><reserved><info_hash><peer_id>`) to authenticate peers.
- **Message protocol**  Full implementation of choke/unchoke/interested/have/bitfield/request/piece/cancel messages.
- **Piece manager**  Assembles blocks into pieces, verifies SHA1 hashes, and writes to disk.
- **Download monitoring**  Real-time progress tracking with speed calculations.
- **Seeding disabled**  Ignores all incoming REQUEST messages from peers (download-only mode).

### Security & Privacy Notes
⚠️ **This client is designed for downloading ONLY:**
- Does NOT respond to peer REQUEST messages
- Does NOT send PIECE messages to peers
- Does NOT unchoke peers who express interest
- Reports 0 bytes uploaded in tracker announcements

---

##  Installation & usage

### Prerequisites

- Python 3.8 or higher.
- An internet connection.

### Setup

Clone the repository. Navigate to the project root:

```bash
cd HiveMind
```

### Running the client

Provide a valid .torrent file and optionally an output path:

```bash
python main.py /path/to/your/file.torrent [output_path]
```

Example output:

```text
🐝 HIVE-MIND CLIENT v1.0 - DOWNLOAD ONLY MODE
==================================================
⚠️  SEEDING DISABLED - This client will NOT upload data
==================================================
📦 TORRENT: ubuntu-25.10-desktop-amd64.iso
   Size: 5438.35 MB
   Pieces: 21754
   Output: ubuntu-25.10-desktop-amd64.iso

📡 Contacting Tracker...
🟢 SUCCESS: Found 50 potential peers.
🌐 Seeders: 2291, Leechers: 80

⚡ CONNECTING TO SWARM (Max 50 concurrent)...
--------------------------------------------------
📊 Progress: 1523/21754 pieces (7.0%) | Downloaded: 256.3 MB | Speed: 512.5 KB/s
```

**Project structure**

```text
HiveMind/
 hive_core/           # Backend logic
    bencoding.py     # Decodes integers, lists, dicts from raw bytes
    torrent.py       # Manages metadata and SHA1 hashing
    tracker.py       # HTTP Multi-Tracker communication logic
    peer.py          # Async TCP Peer Connection & Message Protocol
    piece_manager.py # Piece assembly, verification, and file writing
    utils.py         # ID generation and IP decoding utilities
 main.py              # Entry point
 README.md            # You are here
```

##  Technical Implementation Details

### Download-Only Mode (Seeding Disabled)

The client implements several mechanisms to ensure it never uploads data:

1. **Ignores REQUEST messages**: When a peer requests a piece block, the message is silently discarded
2. **Never sends UNCHOKE**: Even when peers express interest, we never unchoke them
3. **Reports zero upload**: Tracker announcements always report `uploaded=0`
4. **No HAVE messages**: We don't broadcast which pieces we've completed

### Message Protocol

The BitTorrent protocol uses the following message types:
- **0 - CHOKE**: Temporarily stops sending data
- **1 - UNCHOKE**: Resumes sending data  
- **2 - INTERESTED**: Expresses desire to download
- **3 - NOT_INTERESTED**: No longer wants to download
- **4 - HAVE**: Announces possession of a piece
- **5 - BITFIELD**: Initial exchange of available pieces
- **6 - REQUEST**: Requests a block of data
- **7 - PIECE**: Contains requested block data
- **8 - CANCEL**: Cancels a previous request

### Piece Management

1. Blocks are received and stored in memory by piece index and offset
2. When all blocks for a piece are received, they're assembled
3. SHA1 hash is calculated and compared against the expected hash
4. Valid pieces are written to disk at the correct offset
5. Progress is tracked via a bitfield (1 bit per piece)

##  License

MIT License. Educational purposes only. Please respect copyright laws when downloading content.

 Welcome to the swarm
