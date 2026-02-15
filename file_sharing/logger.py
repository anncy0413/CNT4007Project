import logging
import time

class PeerLogger:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        # Kexin: Log file name format: log_peer_[peerID].log
        self.filename = f"log_peer_{peer_id}.log"
        
        # Kexin: Configure logger.
        self.logger = logging.getLogger(f"Peer_{peer_id}")
        self.logger.setLevel(logging.INFO)
        
        handler = logging.FileHandler(self.filename, mode='w') # Kexin: mode='w' overwrites on each run.
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _get_time(self):
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def log_tcp_connect(self, target_peer_id):
        """Kexin: [cite: 189]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} makes a connection to Peer {target_peer_id}."
        self.logger.info(msg)
        print(msg) # Kexin: Optional console output for debugging.

    def log_tcp_connected(self, source_peer_id):
        """Kexin: [cite: 193]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} is connected from Peer {source_peer_id}."
        self.logger.info(msg)
        print(msg)

    def log_change_preferred_neighbors(self, neighbor_list):
        """Kexin: [cite: 197]"""
        neighbors_str = ','.join(map(str, neighbor_list))
        msg = f"{self._get_time()}: Peer {self.peer_id} has the preferred neighbors {neighbors_str}."
        self.logger.info(msg)

    def log_change_optimistically_unchoked(self, neighbor_id):
        """Kexin: [cite: 200]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} has the optimistically unchoked neighbor {neighbor_id}."
        self.logger.info(msg)

    def log_unchoked(self, neighbor_id):
        """Kexin: [cite: 204]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} is unchoked by {neighbor_id}."
        self.logger.info(msg)

    def log_choked(self, neighbor_id):
        """Kexin: [cite: 208]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} is choked by {neighbor_id}."
        self.logger.info(msg)

    def log_receive_have(self, neighbor_id, piece_index):
        """Kexin: [cite: 212]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} received the 'have' message from {neighbor_id} for the piece {piece_index}."
        self.logger.info(msg)

    def log_receive_interested(self, neighbor_id):
        """Kexin: [cite: 217]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} received the 'interested' message from {neighbor_id}."
        self.logger.info(msg)

    def log_receive_not_interested(self, neighbor_id):
        """Kexin: [cite: 221]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} received the 'not interested' message from {neighbor_id}."
        self.logger.info(msg)

    def log_download_piece(self, neighbor_id, piece_index, total_pieces):
        """Kexin: [cite: 224]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} has downloaded the piece {piece_index} from {neighbor_id}. Now the number of pieces it has is {total_pieces}."
        self.logger.info(msg)

    def log_download_complete(self):
        """Kexin: [cite: 231]"""
        msg = f"{self._get_time()}: Peer {self.peer_id} has downloaded the complete file."
        self.logger.info(msg)
