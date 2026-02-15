import os

class Config:
    def __init__(self):
        self.common = {}
        self.peers = []
        self.parse_common()
        self.parse_peer_info()

    def parse_common(self):
        """Kexin: Read Common.cfg. [cite: 103]"""
        filename = 'Common.cfg'
        with open(filename, 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 2:
                    key, value = parts[0], parts[1]
                    if key in ['NumberOfPreferredNeighbors', 'UnchokingInterval', 
                               'OptimisticUnchokingInterval', 'FileSize', 'PieceSize']:
                        self.common[key] = int(value)
                    else:
                        self.common[key] = value

    def parse_peer_info(self):
        """Kexin: Read PeerInfo.cfg. [cite: 118]"""
        filename = 'PeerInfo.cfg'
        with open(filename, 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 4:
                    peer = {
                        'peerId': int(parts[0]),
                        'hostName': parts[1],
                        'port': int(parts[2]),
                        'hasFile': int(parts[3]) == 1
                    }
                    self.peers.append(peer)

    def get_peer_info(self, peer_id):
        for peer in self.peers:
            if peer['peerId'] == peer_id:
                return peer
        return None
