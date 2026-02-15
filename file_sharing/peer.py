import socket
import threading
import time
import struct
import random
import os
import math
import sys
from .protocol import *
from .logger import PeerLogger

class FileManager:
    """Kexin: Manage file I/O and bitfield state."""
    def __init__(self, peer_id, has_file, common_config, logger):
        self.peer_id = peer_id
        self.file_name = common_config['FileName']
        self.file_size = common_config['FileSize']
        self.piece_size = common_config['PieceSize']
        self.logger = logger
        
        # Kexin: Compute total number of pieces.
        self.num_pieces = math.ceil(self.file_size / self.piece_size)
        
        # Kexin: Initialize the bitfield.
        # Kexin: Use a simple list of ints (0 or 1) for readability.
        if has_file:
            self.bitfield = [1] * self.num_pieces
        else:
            self.bitfield = [0] * self.num_pieces
            
        # Kexin: Ensure the peer directory exists.
        self.dir_path = f"peer_{self.peer_id}"
        if not os.path.exists(self.dir_path):
            os.makedirs(self.dir_path)
            
        self.file_path = os.path.join(self.dir_path, self.file_name)
        
        # Kexin: If the peer has the full file, ensure it exists in the directory.
        # Kexin: This implementation assumes the file is already under peer_ID when has_file=True.
        if not has_file:
            # Kexin: Create an empty placeholder file.
            with open(self.file_path, "wb") as f:
                pass # Kexin: Create an empty file for later random-access writes.

        # Kexin: Track requested-but-not-yet-downloaded pieces to avoid duplicates.
        self.requested_pieces = set()

    def has_piece(self, index):
        return self.bitfield[index] == 1

    def has_complete_file(self):
        return all(self.bitfield)

    def get_bitfield_bytes(self):
        """Kexin: Convert the bitfield list to bytes for network transfer."""
        # Kexin: Pack piece availability into bits.
        # Kexin: Follow the project spec: high bit to low bit maps piece 0-7.
        # Kexin: Each byte stores the status of 8 pieces.
        num_bytes = math.ceil(self.num_pieces / 8)
        bf_bytes = bytearray(num_bytes)
        for i in range(self.num_pieces):
            if self.bitfield[i] == 1:
                byte_index = i // 8
                bit_offset = 7 - (i % 8)
                bf_bytes[byte_index] |= (1 << bit_offset)
        return bytes(bf_bytes)

    def update_bitfield_from_bytes(self, bf_data):
        """Kexin: Parse received bytes into a neighbor bitfield for state tracking."""
        neighbor_bitfield = [0] * self.num_pieces
        for i in range(self.num_pieces):
            byte_index = i // 8
            bit_offset = 7 - (i % 8)
            if byte_index < len(bf_data):
                if (bf_data[byte_index] >> bit_offset) & 1:
                    neighbor_bitfield[i] = 1
        return neighbor_bitfield

    def read_piece(self, index):
        """Kexin: Read piece data by index."""
        if not self.has_piece(index):
            return None
            
        offset = index * self.piece_size
        # Kexin: Compute actual piece size; the last piece may be smaller.
        current_piece_size = min(self.piece_size, self.file_size - offset)
        
        try:
            with open(self.file_path, "rb") as f:
                f.seek(offset)
                data = f.read(current_piece_size)
                return data
        except IOError as e:
            print(f"Error reading file: {e}")
            return None

    def write_piece(self, index, data):
        """Kexin: Write piece data by index."""
        offset = index * self.piece_size
        try:
            with open(self.file_path, "r+b") as f:
                f.seek(offset)
                f.write(data)
            
            self.bitfield[index] = 1
            if index in self.requested_pieces:
                self.requested_pieces.remove(index)
            return True
        except IOError as e:
            print(f"Error writing file: {e}")
            return False

class PeerConnection:
    """Kexin: Manage connection state with one neighbor."""
    def __init__(self, socket, peer_id, neighbor_id, peer_instance):
        self.socket = socket
        self.my_peer_id = peer_id
        self.neighbor_id = neighbor_id
        self.peer = peer_instance # Kexin: Reference to the parent Peer instance.
        self.logger = peer_instance.logger
        
        # Kexin: Connection state flags.
        self.am_choking = True        # Kexin: Whether I am choking the neighbor.
        self.am_interested = False    # Kexin: Whether I am interested in the neighbor.
        self.peer_choking = True      # Kexin: Whether the neighbor is choking me.
        self.peer_interested = False  # Kexin: Whether the neighbor is interested in me.
        
        self.neighbor_bitfield = [0] * self.peer.file_manager.num_pieces
        
        # Kexin: Download rate accounting for neighbor selection.
        self.download_rate = 0
        self.downloaded_bytes_interval = 0

    def send_message(self, msg_type, payload=b''):
        """Kexin: Pack and send a protocol message."""
        msg_len = len(payload)
        # Kexin: 4 bytes length (not including length field itself per )
        # Kexin: Per protocol, message length is len(type + payload) = 1 + len(payload).
        packet_len = 1 + len(payload)
        header = struct.pack('!IB', packet_len, msg_type)
        try:
            self.socket.sendall(header + payload)
        except Exception as e:
            print(f"Send error to {self.neighbor_id}: {e}")

    def close(self):
        try:
            self.socket.close()
        except:
            pass

class Peer:
    def __init__(self, peer_id, config, logger):
        self.peer_id = peer_id
        self.config = config
        self.logger = logger
        self.my_info = config.get_peer_info(peer_id)
        
        # Kexin: Initialize file manager.
        self.file_manager = FileManager(peer_id, self.my_info['hasFile'], config.common, logger)
        
        # Kexin: Neighbor connections: {neighbor_id: PeerConnection}.
        self.connections = {}
        self.connections_lock = threading.Lock()
        
        # Kexin: Server socket.
        self.server_socket = None
        
        # Kexin: Runtime flag.
        self.running = True

    def start(self):
        """Kexin: Main entry point for peer startup."""
        # Kexin: 1) Start the server listener thread.
        server_thread = threading.Thread(target=self.start_server_socket)
        server_thread.daemon = True
        server_thread.start()
        
        # Kexin: 2) Wait briefly to ensure the server is ready.
        time.sleep(1)
        
        # Kexin: 3) Connect to all predecessor peers.
        self.connect_to_predecessors()
        
        # Kexin: 4) Start periodic choking and optimistic unchoking threads.
        threading.Thread(target=self.choking_timer, daemon=True).start()
        threading.Thread(target=self.optimistic_unchoking_timer, daemon=True).start()
        
        # Kexin: 5) Run the termination monitor loop.
        self.termination_check_loop()

    def start_server_socket(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind(('', self.my_info['port']))
            self.server_socket.listen(10)
            # Kexin: print(f"Peer {self.peer_id} listening on port {self.my_info['port']}")
            
            while self.running:
                conn, addr = self.server_socket.accept()
                t = threading.Thread(target=self.handle_incoming_connection, args=(conn,))
                t.daemon = True
                t.start()
        except Exception as e:
            print(f"Server error: {e}")

    def connect_to_predecessors(self):
        """Kexin: Connect to all peers that appear before me in PeerInfo.cfg."""
        for peer in self.config.peers:
            if peer['peerId'] == self.peer_id:
                break # Kexin: Stop at myself; only connect to predecessors.
            
            # Kexin: Initiate outgoing connection.
            self.connect_to_peer(peer)

    def connect_to_peer(self, target_peer):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((target_peer['hostName'], target_peer['port']))
            
            # Kexin: 1) Send handshake.
            handshake_msg = create_handshake_message(self.peer_id)
            s.sendall(handshake_msg)
            
            # Kexin: 2) Receive handshake.
            response = self.read_n_bytes(s, 32)
            neighbor_id = parse_handshake_message(response)
            
            if neighbor_id != target_peer['peerId']:
                print(f"Handshake failed: Expected {target_peer['peerId']}, got {neighbor_id}")
                s.close()
                return

            self.logger.log_tcp_connect(neighbor_id)
            
            # Kexin: 3) Create connection state and start receive loop.
            connection = PeerConnection(s, self.peer_id, neighbor_id, self)
            with self.connections_lock:
                self.connections[neighbor_id] = connection
            
            # Kexin: 4) Send bitfield.
            bf_payload = self.file_manager.get_bitfield_bytes()
            connection.send_message(MSG_BITFIELD, bf_payload)
            
            # Kexin: Start message handler thread.
            t = threading.Thread(target=self.message_handler, args=(connection,))
            t.daemon = True
            t.start()
            
        except Exception as e:
            print(f"Connection to {target_peer['peerId']} failed: {e}")

    def handle_incoming_connection(self, socket):
        """Kexin: Handle incoming connections initiated by other peers."""
        try:
            # Kexin: 1) Receive handshake.
            data = self.read_n_bytes(socket, 32)
            neighbor_id = parse_handshake_message(data)
            
            if neighbor_id is None:
                socket.close()
                return

            # Kexin: 2) Send handshake response.
            handshake_msg = create_handshake_message(self.peer_id)
            socket.sendall(handshake_msg)
            
            self.logger.log_tcp_connected(neighbor_id)
            
            # Kexin: 3) Register connection.
            connection = PeerConnection(socket, self.peer_id, neighbor_id, self)
            with self.connections_lock:
                self.connections[neighbor_id] = connection
                
            # Kexin: 4) Send bitfield.
            bf_payload = self.file_manager.get_bitfield_bytes()
            connection.send_message(MSG_BITFIELD, bf_payload)
            
            # Kexin: Start message processing.
            self.message_handler(connection)
            
        except Exception as e:
            print(f"Incoming connection error: {e}")

    def read_n_bytes(self, sock, n):
        """Kexin: Helper to read exactly n bytes from a socket."""
        data = b''
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                raise ConnectionError("Connection closed unexpectedly")
            data += packet
        return data

    def message_handler(self, conn):
        """Kexin: Dedicated receive loop for each connection."""
        try:
            while self.running:
                # Kexin: 1) Read 4-byte message length.
                length_bytes = self.read_n_bytes(conn.socket, 4)
                msg_len = struct.unpack('!I', length_bytes)[0]
                
                # Kexin: 2) Read message body (type + payload).
                if msg_len > 0:
                    msg_body = self.read_n_bytes(conn.socket, msg_len)
                    msg_type = msg_body[0]
                    payload = msg_body[1:]
                    self.process_message(conn, msg_type, payload)
        except Exception as e:
            # Kexin: print(f"Connection lost with {conn.neighbor_id}: {e}")
            pass
        finally:
            conn.close()

    def process_message(self, conn, msg_type, payload):
        """Kexin: Handle protocol messages by type."""
        
        if msg_type == MSG_CHOKE:
            self.logger.log_choked(conn.neighbor_id)
            conn.peer_choking = True
            
        elif msg_type == MSG_UNCHOKE:
            self.logger.log_unchoked(conn.neighbor_id)
            conn.peer_choking = False
            self.request_piece(conn) # Kexin: On unchoke, immediately try requesting a piece.
            
        elif msg_type == MSG_INTERESTED:
            self.logger.log_receive_interested(conn.neighbor_id)
            conn.peer_interested = True
            
        elif msg_type == MSG_NOT_INTERESTED:
            self.logger.log_receive_not_interested(conn.neighbor_id)
            conn.peer_interested = False
            
        elif msg_type == MSG_HAVE:
            piece_index = struct.unpack('!I', payload)[0]
            self.logger.log_receive_have(conn.neighbor_id, piece_index)
            conn.neighbor_bitfield[piece_index] = 1
            
            # Kexin: Check whether this piece is interesting to me.
            if not self.file_manager.has_piece(piece_index):
                conn.send_message(MSG_INTERESTED)
                conn.am_interested = True
            
        elif msg_type == MSG_BITFIELD:
            # Kexin: Parse bitfield.
            conn.neighbor_bitfield = self.file_manager.update_bitfield_from_bytes(payload)
            # Kexin: Check whether the neighbor has interesting pieces.
            interested = False
            for i in range(len(conn.neighbor_bitfield)):
                if conn.neighbor_bitfield[i] == 1 and not self.file_manager.has_piece(i):
                    interested = True
                    break
            
            if interested:
                conn.send_message(MSG_INTERESTED)
                conn.am_interested = True
            else:
                conn.send_message(MSG_NOT_INTERESTED)
                conn.am_interested = False
                
        elif msg_type == MSG_REQUEST:
            # Kexin: Send data only if the neighbor is currently unchoked by me.
            if not conn.am_choking:
                piece_index = struct.unpack('!I', payload)[0]
                data = self.file_manager.read_piece(piece_index)
                if data:
                    # Kexin: Payload: Index(4) + Content
                    resp_payload = struct.pack('!I', piece_index) + data
                    conn.send_message(MSG_PIECE, resp_payload)
                    
                    # Kexin: Update upload counter.
                    conn.downloaded_bytes_interval += len(data)
                    
        elif msg_type == MSG_PIECE:
            piece_index = struct.unpack('!I', payload[:4])[0]
            piece_data = payload[4:]
            
            # Kexin: Write the piece to local file.
            if self.file_manager.write_piece(piece_index, piece_data):
                # Kexin: Record piece download progress.
                total_pieces = sum(self.file_manager.bitfield)
                self.logger.log_download_piece(conn.neighbor_id, piece_index, total_pieces)
                
                # Kexin: Update downloaded bytes for interval accounting.
                conn.downloaded_bytes_interval += len(piece_data)
                
                # Kexin: Broadcast HAVE to all neighbors.
                have_payload = struct.pack('!I', piece_index)
                with self.connections_lock:
                    for peer in self.connections.values():
                        peer.send_message(MSG_HAVE, have_payload)
                        # Kexin: Re-evaluate interest after receiving this piece.
                        # Kexin: A neighbor may no longer have any needed pieces.
                        self.check_interest(peer)
                
                # Kexin: If file download is complete.
                if self.file_manager.has_complete_file():
                    self.logger.log_download_complete()
                    # Kexin: Not-interested signaling is handled by existing interest checks.
                    
                # Kexin: Continue requesting next piece if still unchoked.
                if not conn.peer_choking:
                    self.request_piece(conn)

    def check_interest(self, conn):
        """Kexin: Check whether I am still interested in this neighbor."""
        still_interested = False
        for i in range(len(conn.neighbor_bitfield)):
            if conn.neighbor_bitfield[i] == 1 and not self.file_manager.has_piece(i):
                still_interested = True
                break
        
        if not still_interested and conn.am_interested:
            conn.send_message(MSG_NOT_INTERESTED)
            conn.am_interested = False

    def request_piece(self, conn):
        """Kexin: Request a piece I need, neighbor has, and I have not requested yet."""
        available_pieces = []
        for i in range(len(conn.neighbor_bitfield)):
            if (conn.neighbor_bitfield[i] == 1 and 
                not self.file_manager.has_piece(i) and 
                i not in self.file_manager.requested_pieces):
                available_pieces.append(i)
        
        if available_pieces:
            # Kexin: Randomly select one candidate piece.
            idx = random.choice(available_pieces)
            self.file_manager.requested_pieces.add(idx)
            payload = struct.pack('!I', idx)
            conn.send_message(MSG_REQUEST, payload)

    def choking_timer(self):
        """Kexin: Reselect preferred neighbors every p seconds."""
        interval = self.config.common['UnchokingInterval']
        k = self.config.common['NumberOfPreferredNeighbors']
        
        while self.running:
            time.sleep(interval)
            
            with self.connections_lock:
                interested_neighbors = [c for c in self.connections.values() if c.peer_interested]
                
                if not interested_neighbors:
                    continue
                
                preferred = []
                
                # Kexin: If I already have the complete file, select randomly.
                if self.file_manager.has_complete_file():
                    random.shuffle(interested_neighbors)
                    preferred = interested_neighbors[:k]
                else:
                    # Kexin: Otherwise, sort by interval bytes in descending order.
                    # Kexin: Rate here uses bytes received from each neighbor.
                    interested_neighbors.sort(key=lambda x: x.downloaded_bytes_interval, reverse=True)
                    preferred = interested_neighbors[:k]
                
                preferred_ids = [c.neighbor_id for c in preferred]
                self.logger.log_change_preferred_neighbors(preferred_ids)
                
                # Kexin: Apply choke/unchoke decisions.
                for c in self.connections.values():
                    if c in preferred:
                        if c.am_choking:
                            c.am_choking = False
                            c.send_message(MSG_UNCHOKE)
                    else:
                        # Kexin: Choke non-preferred neighbors unless marked optimistic.
                        # Kexin: Optimistic handling is maintained in a separate timer.
                        if not c.am_choking and not getattr(c, 'is_optimistic', False):
                            c.am_choking = True
                            c.send_message(MSG_CHOKE)
                    
                    # Kexin: Reset interval counter.
                    c.downloaded_bytes_interval = 0

    def optimistic_unchoking_timer(self):
        """Kexin: Reselect one optimistically unchoked neighbor every m seconds."""
        interval = self.config.common['OptimisticUnchokingInterval']
        
        while self.running:
            time.sleep(interval)
            
            with self.connections_lock:
                # Kexin: Candidates are choked neighbors that are interested in us.
                candidates = [c for c in self.connections.values() 
                              if c.am_choking and c.peer_interested]
                
                if candidates:
                    chosen = random.choice(candidates)
                    chosen.is_optimistic = True # Kexin: Mark to avoid immediate re-choke by main timer.
                    
                    self.logger.log_change_optimistically_unchoked(chosen.neighbor_id)
                    
                    if chosen.am_choking:
                        chosen.am_choking = False
                        chosen.send_message(MSG_UNCHOKE)
                    
                    # Kexin: Clear optimistic marks on others; only one optimistic neighbor is kept.
                    for c in self.connections.values():
                        if c != chosen:
                            c.is_optimistic = False

    def termination_check_loop(self):
        """Kexin: Check whether all peers have completed download."""
        while self.running:
            time.sleep(2)
            
            # Kexin: 1) Verify local completion.
            if not self.file_manager.has_complete_file():
                continue
                
            # Kexin: 2) Verify completion status for all neighbors.
            all_neighbors_done = True
            with self.connections_lock:
                for c in self.connections.values():
                    # Kexin: Check whether neighbor bitfield is full.
                    if sum(c.neighbor_bitfield) < self.file_manager.num_pieces:
                        all_neighbors_done = False
                        break
            
            # Kexin: This simplified check assumes full connectivity and full bitfields.
            # Kexin: Exit when all configured peers are connected and appear complete.
            # Kexin: Spec: terminate when all peers have downloaded the complete file.
            if all_neighbors_done and len(self.connections) == len(self.config.peers) - 1:
                print(f"Peer {self.peer_id}: All peers have complete file. Exiting.")
                self.running = False
                # Kexin: Close all sockets.
                for c in self.connections.values():
                    c.close()
                if self.server_socket:
                    self.server_socket.close()
                sys.exit(0)
