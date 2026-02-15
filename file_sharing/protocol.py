import struct

# Kexin: Message type constants.
MSG_CHOKE = 0
MSG_UNCHOKE = 1
MSG_INTERESTED = 2
MSG_NOT_INTERESTED = 3
MSG_HAVE = 4
MSG_BITFIELD = 5
MSG_REQUEST = 6
MSG_PIECE = 7

# Kexin: Handshake message header.
HANDSHAKE_HEADER = b'P2PFILESHARINGPROJ' # Kexin: 18 bytes [cite: 22]

def create_handshake_message(peer_id):
    """
    Kexin: Build the handshake message:
    Kexin: 18-byte header + 10-byte zero padding + 4-byte peer ID.
    Kexin: Total length is 32 bytes. [cite: 22]
    """
    zero_bits = b'\x00' * 10
    return HANDSHAKE_HEADER + zero_bits + struct.pack('!I', peer_id)

def parse_handshake_message(data):
    """Kexin: Parse handshake data and return peer_id; return None if invalid."""
    if len(data) != 32:
        return None
    header = data[:18]
    if header != HANDSHAKE_HEADER:
        return None
    peer_id = struct.unpack('!I', data[28:32])[0]
    return peer_id
