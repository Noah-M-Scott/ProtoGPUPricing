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
NUM_PRODUCERS = 8  # The number of producer threads to expect (threads per node * nodes).
LOG_DIR = "consumer_log"

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
        self.buffer_lock = threading.Lock() # log lock
        self.packet_counts = {}
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
        sentstamp = payload['timestamp']
        timestamp = time.time()
        
        #if self.packet_counts.get(thread_index) == None :
        #    self.packet_counts[thread_index] = payload['packet']
        #
        #
        #if self.packet_counts.get(thread_index) != payload['packet'] :
        #    self.DROP_COUNTER += 1
        #    print(f"packet dropped {self.DROP_COUNTER} on {thread_index} got {payload['packet']} wanted {self.packet_counts.get(thread_index)} (ignore if followed by DONE)")
        #    self.packet_counts[thread_index] = payload['packet']
        #else:
        #    self.packet_counts[thread_index] += 1
        
        
        print(f"Received {len(payload['data']['ciphertext'])} long payload from {thread_index}; at {timestamp}, was sent at {sentstamp}")
        
        log_file = os.path.join(LOG_DIR, "consumer_log.csv") 
        
        # Lock the specific file for this thread to prevent race conditions
        with self.buffer_lock:
            with open(log_file, 'a') as f:
                f.write(f"{thread_index}, {sentstamp}, {timestamp}\n")
            
        # print(f"[Consumer Server] Wrote record from Producer {thread_index} to {buffer_file}")

    def start(self):
        """Starts the consumer server."""
        # Create directory for buffers if it doesn't exist
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
            print(f"[Consumer Server] Created directory for log: {LOG_DIR}")
        
        # --- prep log file ---
        log_file = os.path.join(LOG_DIR, "consumer_log.csv")
        with open(log_file, 'w') as f:
            f.write("id, sent_time, recv_time\n")

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
    
