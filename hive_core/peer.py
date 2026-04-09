# hive_core/peer.py
import asyncio
import struct
import logging
import hashlib

log = logging.getLogger("Peer")

# BitTorrent Message IDs
MSG_CHOKE = 0
MSG_UNCHOKE = 1
MSG_INTERESTED = 2
MSG_NOT_INTERESTED = 3
MSG_HAVE = 4
MSG_BITFIELD = 5
MSG_REQUEST = 6
MSG_PIECE = 7
MSG_CANCEL = 8

class PeerConnection:
    def __init__(self, ip, port, torrent, my_peer_id, piece_manager=None):
        self.ip = ip
        self.port = port
        self.torrent = torrent
        self.my_peer_id = my_peer_id
        self.piece_manager = piece_manager
        
        self.reader = None
        self.writer = None
        self.connected = False
        self.peer_choking = True  # By default, peers choke us (refuse to give data)
        self.peer_interested = False
        self.am_choking = True  # We choke them by default (but we never upload anyway)
        self.am_interested = False
        self.bitfield = bytearray()
        self.peer_pieces = set()  # Pieces this peer has
        self.download_only = True  # NEVER seed - download only mode
        self.downloaded_count = 0
        self.running = True
        
        # Stats
        self.bytes_downloaded = 0
        self.pieces_received = 0

    async def start(self):
        """
        Main entry point. Connects, Handshakes, then starts message loop for downloading.
        """
        try:
            # 1. Establish TCP Connection (5 second timeout)
            await asyncio.wait_for(self._connect(), timeout=5.0)
            
            # 2. Perform Handshake
            success = await self._handshake()
            if not success:
                await self.close()
                return

            log.info(f"✅ HANDSHAKE SUCCESS: {self.ip}:{self.port}")
            self.connected = True
            
            # Send interested message (we want to download)
            await self._send_message(MSG_INTERESTED, b'')
            self.am_interested = True
            
            log.info(f"⏳ Waiting for bitfield from {self.ip}...")
            
            # Start the message listen loop
            await self._message_loop()

        except asyncio.TimeoutError:
            pass # Peer is offline or firewalled
        except ConnectionRefusedError:
            pass # Peer rejected us
        except Exception as e:
            log.debug(f"Error with {self.ip}: {e}")
            await self.close()

    async def _connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.ip, self.port)

    async def _handshake(self):
        """
        Sends and verifies the BitTorrent Handshake.
        """
        # --- BUILD PACKET ---
        pstr = b"BitTorrent protocol"
        pstrlen = len(pstr) # 19
        reserved = b'\x00' * 8
        
        handshake_packet = (
            bytes([pstrlen]) + 
            pstr + 
            reserved + 
            self.torrent.info_hash_bytes + 
            self.my_peer_id
        )
        
        # --- SEND ---
        self.writer.write(handshake_packet)
        await self.writer.drain()
        
        # --- RECEIVE ---
        # We expect exactly 68 bytes back
        try:
            data = await self.reader.readexactly(68)
        except:
            return False
        
        # --- VERIFY ---
        # 1. Check Protocol Header
        if data[0:20] != bytes([19]) + b"BitTorrent protocol":
            return False

        # 2. Check Info Hash (Bytes 28 to 48)
        # This is CRITICAL. If this doesn't match, they have a different file.
        recv_info_hash = data[28:48]
        if recv_info_hash != self.torrent.info_hash_bytes:
            log.warning(f"{self.ip} - Wrong Info Hash.")
            return False

        # Store peer ID from handshake (bytes 48-68)
        self.peer_id = data[48:68]
        
        return True

    async def _send_message(self, msg_id, payload):
        """Send a BitTorrent protocol message."""
        if not self.writer or not self.connected:
            return
        try:
            # Message format: <length><id><payload>
            length = 1 + len(payload)  # 1 byte for id + payload
            message = struct.pack('>I', length) + bytes([msg_id]) + payload
            self.writer.write(message)
            await self.writer.drain()
        except Exception as e:
            log.debug(f"Error sending message to {self.ip}: {e}")
            await self.close()

    async def _request_piece(self, piece_index, block_offset, block_length):
        """Request a specific block of data from a piece."""
        payload = struct.pack('>III', piece_index, block_offset, block_length)
        await self._send_message(MSG_REQUEST, payload)

    async def _message_loop(self):
        """
        Main message handling loop. Listens for messages from peer and handles them.
        NEVER sends data (seeding disabled).
        """
        while self.running and self.connected:
            try:
                # Read message length (4 bytes, big-endian)
                length_bytes = await asyncio.wait_for(
                    self.reader.readexactly(4), timeout=30.0
                )
                length = struct.unpack('>I', length_bytes)[0]
                
                if length == 0:
                    # Keep-alive message
                    continue
                
                # Read message ID and payload
                message_data = await asyncio.wait_for(
                    self.reader.readexactly(length), timeout=30.0
                )
                msg_id = message_data[0]
                payload = message_data[1:]
                
                log.debug(f"📨 Received message ID {msg_id} from {self.ip} (payload: {len(payload)} bytes)")
                
                await self._handle_message(msg_id, payload)
                
            except asyncio.TimeoutError:
                # No message received in timeout period, send keep-alive
                try:
                    self.writer.write(struct.pack('>I', 0))  # Keep-alive
                    await self.writer.drain()
                except:
                    break
            except Exception as e:
                log.debug(f"Message loop error with {self.ip}: {e}")
                break
        
        await self.close()

    async def _handle_message(self, msg_id, payload):
        """Handle incoming BitTorrent messages."""
        if msg_id == MSG_CHOKE:
            log.debug(f"{self.ip} choked us")
            self.peer_choking = True
            
        elif msg_id == MSG_UNCHOKE:
            log.info(f"🟢 {self.ip} unchoked us - requesting pieces!")
            self.peer_choking = False
            # Send initial requests when unchoked - even without bitfield we can request common pieces
            if self.piece_manager:
                log.info(f"🚀 Peer unchoked! Requesting pieces from {self.ip}")
                # Request first 10 pieces as starter (we'll refine once we get bitfield)
                for piece_idx in range(min(10, self.torrent.total_pieces)):
                    await self._request_blocks_for_piece(piece_idx, max_blocks=2)
                await self._send_initial_requests()
                await self._request_more_pieces()
                
        elif msg_id == MSG_INTERESTED:
            log.debug(f"{self.ip} is interested in what we have")
            self.peer_interested = True
            # NOTE: We NEVER respond with unchoke (seeding disabled)
            
        elif msg_id == MSG_NOT_INTERESTED:
            log.debug(f"{self.ip} is not interested")
            self.peer_interested = False
            
        elif msg_id == MSG_HAVE:
            if len(payload) >= 4:
                piece_index = struct.unpack('>I', payload)[0]
                self.peer_pieces.add(piece_index)
                log.debug(f"{self.ip} has piece {piece_index}")
                
        elif msg_id == MSG_BITFIELD:
            self.bitfield = bytearray(payload)
            log.info(f"📋 Received BITFIELD from {self.ip} ({len(payload)} bytes)")
            # Decode bitfield to know which pieces peer has
            for i, byte in enumerate(self.bitfield):
                for bit in range(8):
                    if byte & (1 << (7 - bit)):
                        piece_index = i * 8 + bit
                        if piece_index < self.torrent.total_pieces:
                            self.peer_pieces.add(piece_index)
            log.info(f"📋 {self.ip} has {len(self.peer_pieces)} pieces")
            # After receiving bitfield, ALWAYS send requests (peer is usually unchoked by default)
            if self.piece_manager:
                # Wait a bit for peer_pieces to be populated before requesting
                await asyncio.sleep(0.1)
                log.info(f"🚀 Sending initial requests to {self.ip} (have {len(self.peer_pieces)} pieces)")
                await self._send_initial_requests()
                await self._request_more_pieces()
            
        elif msg_id == MSG_REQUEST:
            # INCOMING REQUEST FROM PEER - IGNORE (seeding disabled!)
            # We never upload data to peers
            log.debug(f"Ignoring request from {self.ip} (download-only mode)")
            pass
            
        elif msg_id == MSG_PIECE:
            # Received piece data!
            if len(payload) >= 8:
                piece_index, block_offset = struct.unpack('>II', payload[:8])
                block_data = payload[8:]
                self.bytes_downloaded += len(block_data)
                log.debug(f"📥 Received {len(block_data)} bytes for piece {piece_index}")
                
                if self.piece_manager:
                    await self.piece_manager.add_block(piece_index, block_offset, block_data)
                    # Request more pieces after receiving a block (keep the pipeline full)
                    await self._request_more_pieces()
                    
        elif msg_id == MSG_CANCEL:
            log.debug(f"{self.ip} cancelled a request")

    async def _send_initial_requests(self):
        """Send initial piece requests after handshake/bitfield exchange."""
        if not self.piece_manager or self.peer_choking:
            return
            
        # Get pieces this peer has that we need
        needed_pieces = self.peer_pieces.intersection(
            self.piece_manager.get_needed_pieces()
        )
        
        log.info(f"📤 Found {len(needed_pieces)} needed pieces from {self.ip} (peer has {len(self.peer_pieces)}, we need {len(self.piece_manager.get_needed_pieces())})")
        
        if not needed_pieces:
            # If peer has no pieces we need, try requesting anyway in case bitfield wasn't fully parsed
            if len(self.peer_pieces) > 0:
                log.warning(f"⚠️  No overlap in pieces - trying first 5 pieces from peer")
                for piece_idx in list(self.peer_pieces)[:5]:
                    await self._request_blocks_for_piece(piece_idx, max_blocks=3)
            else:
                log.warning(f"⚠️  Peer has no pieces reported")
            return
            
        # Request blocks from available pieces (limit concurrent requests)
        for piece_idx in list(needed_pieces)[:5]:  # Max 5 pieces at a time
            log.debug(f"Requesting piece {piece_idx} from {self.ip}")
            await self._request_blocks_for_piece(piece_idx, max_blocks=3)

    async def _request_more_pieces(self):
        """Request more pieces to keep the download pipeline full."""
        if not self.piece_manager or self.peer_choking:
            return
            
        # Get pieces this peer has that we need
        needed_pieces = self.peer_pieces.intersection(
            self.piece_manager.get_needed_pieces()
        )
        
        if not needed_pieces:
            return
            
        # Request one more piece to keep pipeline full
        for piece_idx in list(needed_pieces)[:1]:
            await self._request_blocks_for_piece(piece_idx, max_blocks=2)

    async def _request_blocks_for_piece(self, piece_idx, max_blocks=3):
        """Request multiple blocks for a specific piece."""
        piece_length = self.torrent.piece_length
        num_blocks = (piece_length // 16384) + (1 if piece_length % 16384 else 0)
        
        for block_num in range(min(num_blocks, max_blocks)):
            offset = block_num * 16384
            length = min(16384, piece_length - offset)
            await self._request_piece(piece_idx, offset, length)

    async def close(self):
        self.running = False
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except:
                pass
        self.connected = False