# main.py
import sys
import asyncio
import logging
import os
from hive_core.torrent import Torrent
from hive_core.tracker import TrackerManager
from hive_core.utils import generate_peer_id
from hive_core.peer import PeerConnection
from hive_core.piece_manager import PieceManager

# Configure nicer logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s',
)

async def process_peer(semaphore, ip, port, torrent, my_id, piece_manager):
    """
    Limits concurrent connections using a semaphore.
    """
    async with semaphore:
        peer = PeerConnection(ip, port, torrent, my_id, piece_manager)
        await peer.start()

async def download_monitor(piece_manager, timeout=60):
    """Monitor and display download progress."""
    start_time = asyncio.get_event_loop().time()
    last_stats = None
    
    while True:
        await asyncio.sleep(2)
        stats = piece_manager.get_download_progress()
        
        elapsed = asyncio.get_event_loop().time() - start_time
        if stats != last_stats:
            speed = stats['bytes_downloaded'] / elapsed if elapsed > 0 else 0
            print(f"\r📊 Progress: {stats['pieces_completed']}/{stats['total_pieces']} pieces "
                  f"({stats['percent_complete']:.1f}%) | "
                  f"Downloaded: {stats['bytes_downloaded']/1024:.1f} KB | "
                  f"Speed: {speed/1024:.1f} KB/s", end='', flush=True)
            last_stats = stats
        
        if stats['pieces_completed'] >= stats['total_pieces']:
            print("\n✅ Download complete!")
            break
            
        if elapsed > timeout:
            print(f"\n⏱️  Timeout reached ({timeout}s)")
            break

async def main():
    print("🐝 HIVE-MIND CLIENT v1.0 - DOWNLOAD ONLY MODE")
    print("=" * 50)
    print("⚠️  SEEDING DISABLED - This client will NOT upload data")
    print("=" * 50)
    
    if len(sys.argv) < 2:
        print("Usage: python main.py <file.torrent> [output_path]")
        return

    try:
        # 1. Load Torrent
        t_path = sys.argv[1]
        torrent = Torrent(t_path)
        print(f"📦 TORRENT: {torrent.name}")
        print(f"   Size: {torrent.total_length / (1024*1024):.2f} MB")
        print(f"   Pieces: {torrent.total_pieces}")
        
        # Determine output path
        output_path = sys.argv[2] if len(sys.argv) > 2 else torrent.name
        print(f"   Output: {output_path}")
        
        # 2. Identity
        my_id = generate_peer_id()
        
        # 3. Initialize Piece Manager
        piece_manager = PieceManager(torrent, output_path)
        
        # 4. Tracker
        print("\n📡 Contacting Tracker...")
        tracker = TrackerManager(torrent, my_id)
        peers_list = tracker.connect()

        if not peers_list:
            print("🔴 FAILURE: No peers found.")
            return

        print(f"🟢 SUCCESS: Found {len(peers_list)} potential peers.")
        print(f"🌐 Seeders: {tracker.seeders}, Leechers: {tracker.leechers}")
        print("\n⚡ CONNECTING TO SWARM (Max 50 concurrent)...")
        print("-" * 50)

        # 5. Async Connection Swarm
        sem = asyncio.Semaphore(50) # Allow 50 parallel connections
        tasks = []
        
        for ip, port in peers_list:
            task = asyncio.create_task(
                process_peer(sem, ip, port, torrent, my_id, piece_manager)
            )
            tasks.append(task)

        # Add download monitor task
        monitor_task = asyncio.create_task(download_monitor(piece_manager, timeout=120))
        
        # Run all peer connections and monitor
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Cancel monitor if still running
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        
        print("\n" + "-" * 50)
        final_stats = piece_manager.get_download_progress()
        print(f"📊 FINAL STATS:")
        print(f"   Pieces: {final_stats['pieces_completed']}/{final_stats['total_pieces']}")
        print(f"   Downloaded: {final_stats['bytes_downloaded']/1024/1024:.2f} MB")
        print(f"   Blocks Received: {final_stats['blocks_received']}")
        print(f"   Uploaded: 0 bytes (seeding disabled)")
        
        if final_stats['pieces_completed'] >= final_stats['total_pieces']:
            print("\n✅ DOWNLOAD COMPLETE!")
        else:
            print(f"\n⚠️  Download incomplete ({final_stats['percent_complete']:.1f}%)")

    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping...")