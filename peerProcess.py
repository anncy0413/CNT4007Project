import sys
import os
from file_sharing.config import Config
from file_sharing.logger import PeerLogger
# Kexin: Import the core Peer class.
from file_sharing.peer import Peer

def main():
    # Kexin: Validate command-line arguments.
    if len(sys.argv) != 2:
        print("Usage: python peerProcess.py <peerID>")
        sys.exit(1)

    try:
        my_peer_id = int(sys.argv[1])
    except ValueError:
        print("Peer ID must be an integer.")
        sys.exit(1)

    # Kexin: 1) Load configuration.
    # Kexin: Ensure configuration files exist.
    if not os.path.exists('Common.cfg') or not os.path.exists('PeerInfo.cfg'):
        print("Error: Common.cfg or PeerInfo.cfg not found.")
        sys.exit(1)

    config = Config()
    my_config = config.get_peer_info(my_peer_id)
    
    if not my_config:
        print(f"Peer ID {my_peer_id} not found in PeerInfo.cfg")
        sys.exit(1)
        
    print(f"Starting Peer {my_peer_id} at {my_config['hostName']}:{my_config['port']}")
    print(f"Has file: {my_config['hasFile']}")

    # Kexin: 2) Initialize logger.
    logger = PeerLogger(my_peer_id)
    
    try:
        # Kexin: 3) Initialize and start the peer core logic.
        peer = Peer(my_peer_id, config, logger)
        peer.start()
        
    except KeyboardInterrupt:
        print("Shutting down...")
        sys.exit(0)
    except Exception as e:
        print(f"An error occurred: {e}")
        # Kexin: In production, write the exception to the log.
        # Kexin: logger.logger.error(f"Critical error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
