import socket
import threading
import time
import pickle
import os
import random
import struct
from datetime import datetime

# ==============================================================================
#  CONSTANT VARIABLES (Editable)
# ==============================================================================

# --- General ---
HOST = '127.0.0.1'  # Server host
PORT = 65432        # Server port to listen on


# --- Consumer (Type B) Constants ---
NUM_PRODUCERS = 4  # The number of producer threads to expect (threads per node * nodes).
X_RECORDS = 64  # Max number of records in the on-disk circular buffer.
BUFFER_DIR = "circular_buffers" # Directory to store buffer files.


# ==============================================================================
#  Consumer Thread
# ==============================================================================

class ConsumerServer:
    """
    The main class for the consumer server.
    - Listens for connections from producers.
    - Spawns handler threads for each connection.
    - Manages the lifecycle and shutdown process.
    """
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.producers_done_count = 0
        self.DROP_COUNTER = 0
        self.lock = threading.Lock() # To protect the done count
        self.buffer_locks = {i: threading.Lock() for i in range(NUM_PRODUCERS)} # Per-file locks
        self.packet_counts = [0] * NUM_PRODUCERS
        self.shutdown_event = threading.Event()

    def _increment_done_count(self):
        with self.lock:
            self.producers_done_count += 1
            print(f"[Consumer Server] 'DONE' message received. Total done: {self.producers_done_count}/{NUM_PRODUCERS}")
        if self.producers_done_count >= NUM_PRODUCERS:
            print("[Consumer Server] All producers have finished. Initiating shutdown.")
            self.shutdown_event.set()

    def _handle_client(self, conn, addr):
        """
        This function is executed in a separate thread for each connected producer.
        """
        print(f"[Consumer Handler] Accepted connection from {addr}")
        client_active = True
        while client_active and not self.shutdown_event.is_set():
            try:
                data = conn.recv(4) # Receive data in chunks
                data_length = struct.unpack("!I", data)[0]
                
                received_data = b""
                while len(received_data) < data_length:
                    chunk = conn.recv(data_length - len(received_data))
                    if not chunk: # Connection closed unexpectedly
                        break
                    received_data += chunk
                
                
                if len(received_data) == data_length :
                    payload = pickle.loads(received_data)
                    
                    # Check for termination message
                    if isinstance(payload, str) and payload.startswith("DONE"):
                        client_active = False
                        self._increment_done_count()
                        break
                    else:
                        self._process_payload(payload)

                else:
                    # No data received, connection might be closed
                    client_active = False
                    print(f"[Consumer Handler] Connection from {addr} closed unexpectedly.")
                    # We might need a more robust way to signal this producer is done
                    # For now, we assume it will always send a DONE message.

            except (pickle.UnpicklingError, EOFError):
                # This can happen if data stream is cut mid-object
                print(f"[WARNING][Consumer Handler] Could not decode data from {addr}. Connection may have dropped.")
                client_active = False
            except Exception as e:
                print(f"[ERROR][Consumer Handler] Error handling client {addr}: {e}")
                client_active = False
        
        conn.close()
        print(f"[Consumer Handler] Closed connection from {addr}.")
    
    def _process_payload(self, payload: dict):
        """
        Processes a received data payload and writes it to the on-disk buffer.
        """
        thread_index = payload['index']
        timestamp = datetime.now().isoformat()
        
        if self.packet_counts[thread_index] != payload['packet'] :
            self.DROP_COUNTER += 1
            print(f"packet dropped {self.DROP_COUNTER} on {thread_index} got {payload['packet']} wanted {self.packet_counts[thread_index]} (ignore if followed by DONE)")
            self.packet_counts[thread_index] = payload['packet']
        else:
            self.packet_counts[thread_index] += 1
        
        record_to_add = {
            "timestamp": timestamp,
            "amountOwed": payload['amountOwed'],
            "data": payload['data']
        }
        
        print(f"Received {len(payload['data'])} long payload from {thread_index}; current data input per second is X bytes (DATA VOLUME MEASURE POINT #2)")
        
        buffer_file = os.path.join(BUFFER_DIR, f"buffer_{thread_index}.pkl")
        
        # Lock the specific file for this thread to prevent race conditions
        print("Writing to disk... current database size is X kilobytes (DATA VOLUME MEASURE POINT #3)")
        with self.buffer_locks[thread_index]:
            # --- THIS IS THE INEFFICIENT PART AS REQUESTED ---
            # 1. Read the entire existing buffer from disk
            try:
                with open(buffer_file, 'rb') as f:
                    circular_buffer = pickle.load(f)
            except (FileNotFoundError, EOFError):
                circular_buffer = []

            # 2. Update the buffer in memory
            circular_buffer.append(record_to_add)

            # 3. Enforce circular buffer size limit
            if len(circular_buffer) > X_RECORDS:
                circular_buffer = circular_buffer[-X_RECORDS:]

            # 4. Write the entire updated buffer back to disk
            with open(buffer_file, 'wb') as f:
                pickle.dump(circular_buffer, f)
            # --- END OF INEFFICIENT PART ---

        # print(f"[Consumer Server] Wrote record from Producer {thread_index} to {buffer_file}")

    def start(self):
        """Starts the consumer server."""
        # Create directory for buffers if it doesn't exist
        if not os.path.exists(BUFFER_DIR):
            os.makedirs(BUFFER_DIR)
            print(f"[Consumer Server] Created directory for buffers: {BUFFER_DIR}")

        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(NUM_PRODUCERS)
        # Set a timeout so the accept() call doesn't block forever
        self.server_socket.settimeout(1.0) 
        print(f"[Consumer Server] Listening on {self.host}:{self.port}")

        while not self.shutdown_event.is_set():
            try:
                conn, addr = self.server_socket.accept()
                # Spawn a new thread to handle this client
                handler_thread = threading.Thread(target=self._handle_client, args=(conn, addr))
                handler_thread.daemon = True # Allows main thread to exit even if handlers are running
                handler_thread.start()
            except socket.timeout:
                # This is expected, just loop again to check the shutdown_event
                continue
        
        print("[Consumer Server] Shutdown signal received. Closing server socket.")
        self.server_socket.close()


if __name__ == "__main__":
    
    # --- Start Consumer ---
    consumer_server = ConsumerServer(HOST, PORT)
    consumer_thread = threading.Thread(target=consumer_server.start)
    consumer_thread.start()

    # Give the server a moment to start up
    time.sleep(1)
    
    
    consumer_thread.join()

    print("\n[Main] All threads have completed their execution.")
    
