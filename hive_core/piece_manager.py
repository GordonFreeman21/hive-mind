# hive_core/piece_manager.py
import asyncio
import hashlib
import logging
import os

log = logging.getLogger("PieceManager")

class PieceManager:
    """
    Manages piece download progress, block assembly, and file writing.
    """
    
    def __init__(self, torrent, output_path=None):
        self.torrent = torrent
        self.piece_length = torrent.piece_length
        self.total_pieces = torrent.total_pieces
        
        # Track which pieces we have (bitfield)
        self.pieces_bitfield = bytearray((self.total_pieces + 7) // 8)
        
        # Store blocks for each piece until complete
        # Format: {piece_index: {block_offset: block_data}}
        self.pending_pieces = {}
        
        # Track needed pieces (all initially)
        self.needed_pieces = set(range(self.total_pieces))
        
        # Output file handling
        self.output_path = output_path or torrent.name
        self.file_handle = None
        
        # Stats
        self.bytes_downloaded = 0
        self.pieces_completed = 0
        self.blocks_received = 0
        
        # Lock for thread-safe operations
        self.lock = asyncio.Lock()
        
        # Completion event
        self.download_complete = asyncio.Event()

    def get_needed_pieces(self):
        """Return set of piece indices we still need."""
        return self.needed_pieces.copy()

    def has_piece(self, piece_index):
        """Check if we have a complete piece."""
        byte_index = piece_index // 8
        bit_index = 7 - (piece_index % 8)
        return bool(self.pieces_bitfield[byte_index] & (1 << bit_index))

    def mark_piece_complete(self, piece_index):
        """Mark a piece as complete in our bitfield."""
        byte_index = piece_index // 8
        bit_index = 7 - (piece_index % 8)
        self.pieces_bitfield[byte_index] |= (1 << bit_index)
        self.needed_pieces.discard(piece_index)
        self.pieces_completed += 1
        log.info(f"📊 Progress: {self.pieces_completed}/{self.total_pieces} pieces ({100*self.pieces_completed/self.total_pieces:.1f}%)")

    async def add_block(self, piece_index, block_offset, block_data):
        """
        Add a received block to a pending piece.
        Verify and write piece when complete.
        """
        async with self.lock:
            self.blocks_received += 1
            self.bytes_downloaded += len(block_data)
            
            # Initialize pending piece if needed
            if piece_index not in self.pending_pieces:
                self.pending_pieces[piece_index] = {}
            
            # Store the block
            self.pending_pieces[piece_index][block_offset] = block_data
            
            # Check if piece is complete
            if self._is_piece_complete(piece_index):
                await self._assemble_and_verify_piece(piece_index)

    def _is_piece_complete(self, piece_index):
        """Check if all blocks for a piece have been received."""
        if piece_index not in self.pending_pieces:
            return False
            
        # Calculate expected piece length
        is_last_piece = piece_index == self.total_pieces - 1
        expected_length = (
            self.torrent.total_length - (piece_index * self.piece_length)
            if is_last_piece
            else self.piece_length
        )
        
        # Calculate received length
        blocks = self.pending_pieces[piece_index]
        received_length = sum(len(data) for data in blocks.values())
        
        return received_length >= expected_length

    async def _assemble_and_verify_piece(self, piece_index):
        """Assemble blocks into a piece and verify hash."""
        blocks = self.pending_pieces[piece_index]
        
        # Sort blocks by offset and concatenate
        sorted_offsets = sorted(blocks.keys())
        piece_data = b''.join(blocks[offset] for offset in sorted_offsets)
        
        # Remove from pending
        del self.pending_pieces[piece_index]
        
        # Verify hash
        expected_hash = self.torrent.piece_hashes[piece_index]
        actual_hash = hashlib.sha1(piece_data).digest()
        
        if actual_hash != expected_hash:
            log.warning(f"❌ Piece {piece_index} failed hash check!")
            # Could re-request here, but for now just discard
            return False
        
        log.info(f"✅ Piece {piece_index} verified!")
        
        # Mark as complete
        self.mark_piece_complete(piece_index)
        
        # Write to file
        await self._write_piece(piece_index, piece_data)
        
        # Check if download is complete
        if not self.needed_pieces:
            log.info("🎉 DOWNLOAD COMPLETE!")
            self.download_complete.set()
        
        return True

    async def _write_piece(self, piece_index, piece_data):
        """Write a completed piece to the output file."""
        try:
            # For single-file torrents, write directly
            if len(self.torrent.files) == 1:
                async with self.lock:
                    # Open file in read-write mode
                    mode = 'r+b' if os.path.exists(self.output_path) else 'wb'
                    with open(self.output_path, mode) as f:
                        # Seek to position and write
                        f.seek(piece_index * self.piece_length)
                        f.write(piece_data)
            else:
                # Multi-file torrent - handle differently
                # For now, just append to a single file
                async with self.lock:
                    with open(self.output_path, 'ab') as f:
                        f.write(piece_data)
                        
        except Exception as e:
            log.error(f"Error writing piece {piece_index}: {e}")

    def get_download_progress(self):
        """Return download statistics."""
        return {
            'pieces_completed': self.pieces_completed,
            'total_pieces': self.total_pieces,
            'bytes_downloaded': self.bytes_downloaded,
            'blocks_received': self.blocks_received,
            'percent_complete': 100 * self.pieces_completed / self.total_pieces if self.total_pieces > 0 else 0,
            'needed_pieces_count': len(self.needed_pieces)
        }

    async def wait_for_completion(self, timeout=None):
        """Wait for download to complete."""
        try:
            await asyncio.wait_for(self.download_complete.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def close(self):
        """Cleanup resources."""
        if self.file_handle:
            self.file_handle.close()
