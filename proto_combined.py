import socket
import threading
import time
import pickle
import os
import random
from datetime import datetime

# ==============================================================================
#  CONSTANT VARIABLES (Editable)
# ==============================================================================

# --- General ---
HOST = '127.0.0.1'  # Server host
PORT = 65432        # Server port to listen on

# --- Producer (Type A) Constants ---
NUM_PRODUCERS = 1  # The number of producer threads to create.
# Array of trace file names. Must have at least NUM_PRODUCERS elements.
TRACE_FILES = [f"trace_{i}.trace" for i in range(NUM_PRODUCERS)]
N_MICROSECONDS = 100    # Interval to read from trace file (1000 us = 1 ms).
M_MICROSECONDS = 500    # Interval to process data and send (5000 us = 5 ms).
K_ITEMS = 128          # Number of items in the temporary list before sending.

# --- Consumer (Type B) Constants ---
X_RECORDS = 64  # Max number of records in the on-disk circular buffer.
BUFFER_DIR = "circular_buffers" # Directory to store buffer files.

# ==============================================================================
#  Placeholder Pricing Function
# ==============================================================================

def pricing_function(bandwidth: int, compute: int) -> float:
    """
    An empty function to calculate a price based on bandwidth and compute.
    This can be replaced with a real pricing model.
    For this example, it returns a simple calculated value.
    """
    # Example: price is a combination of bandwidth and compute usage
    price = (bandwidth * 0.01) + (compute * 0.005)
    return price

# ==============================================================================
#  Producer Thread (Type A)
# ==============================================================================

def producer_thread_func(index: int):
    """
    The main function for a producer thread.
    - Connects to the consumer's socket.
    - Reads data from its assigned trace file.
    - Periodically processes and sends data to the consumer.
    """
    print(f"[Producer {index}] Starting.")
    thread_name = f"Producer {index}"

    try:
        # --- File and variable setup ---
        trace_file_name = TRACE_FILES[index]
        with open(trace_file_name, 'r') as f:
            # Read all comma-separated values at once and create an iterator
            values = iter(f.read().split(','))

        # --- Connect to consumer socket ---
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((HOST, PORT))
        print(f"[{thread_name}] Connected to consumer at {HOST}:{PORT}")

        # --- State variables ---
        temp_list = []
        amount_owed = 0.0
        bandwidth, compute = 0, 0
        
        last_read_time = time.perf_counter()
        last_process_time = time.perf_counter()

        # --- Main loop ---
        while True:
            current_time = time.perf_counter()

            # --- Task 1: Read from trace file every N microseconds ---
            if (current_time - last_read_time) * 1_000_000 >= N_MICROSECONDS:
                try:
                    bandwidth = int(next(values))
                    compute = int(next(values))
                    # print(f"[{thread_name}] Read: bw={bandwidth}, comp={compute}") # Uncomment for verbose logging
                    last_read_time = current_time
                except StopIteration:
                    # End of file reached
                    print(f"[{thread_name}] Reached end of trace file.")
                    break # Exit the main while loop

            # --- Task 2: Process and potentially send data every M microseconds ---
            if (current_time - last_process_time) * 1_000_000 >= M_MICROSECONDS:
                if bandwidth is not None and compute is not None:
                    # Accumulate amount owed
                    amount_owed += pricing_function(bandwidth, compute)
                    # Add to temporary list
                    temp_list.append((bandwidth, compute))

                    # Check if the list is ready to be sent
                    if len(temp_list) >= K_ITEMS:
                        payload = {
                            "index": index,
                            "amountOwed": amount_owed,
                            "data": temp_list
                        }
                        serialized_payload = pickle.dumps(payload)
                        
                        print(f"[{thread_name}] Sending batch of {len(temp_list)} items.")
                        client_socket.sendall(serialized_payload)
                        
                        # Reset the temporary list for the next batch
                        temp_list = []
                
                last_process_time = current_time

            # A small sleep to prevent the loop from busy-waiting and consuming 100% CPU
            time.sleep(0.0001)
        
        # --- Termination ---
        # Send any remaining data before closing
        if temp_list:
            payload = {
                "index": index,
                "amountOwed": amount_owed,
                "data": temp_list
            }
            serialized_payload = pickle.dumps(payload)
            print(f"[{thread_name}] Sending final batch of {len(temp_list)} items.")
            client_socket.sendall(serialized_payload)
            
        # Send the "I'm done" message
        print(f"[{thread_name}] Sending 'I'm done' message and shutting down.")
        done_message = pickle.dumps(f"DONE:{index}")
        client_socket.sendall(done_message)
        client_socket.close()
        print(f"[{thread_name}] Finished.")
        
        
    except FileNotFoundError:
        print(f"[ERROR][{thread_name}] Trace file not found: {trace_file_name}")
    except ConnectionRefusedError:
        print(f"[ERROR][{thread_name}] Connection refused. Is the consumer server running?")


# ==============================================================================
#  Consumer Thread (Type B)
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
        self.lock = threading.Lock() # To protect the done count
        self.buffer_locks = {i: threading.Lock() for i in range(NUM_PRODUCERS)} # Per-file locks
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
                data = conn.recv(4096) # Receive data in chunks
                if data:
                    payload = pickle.loads(data)
                    
                    # Check for termination message
                    if isinstance(payload, str) and payload.startswith("DONE"):
                        client_active = False
                        self._increment_done_count()
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
        
        record_to_add = {
            "timestamp": timestamp,
            "amountOwed": payload['amountOwed'],
            "data": payload['data']
        }

        buffer_file = os.path.join(BUFFER_DIR, f"buffer_{thread_index}.pkl")
        
        # Lock the specific file for this thread to prevent race conditions
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


# ==============================================================================
#  Utility and Main Execution
# ==============================================================================

def generate_dummy_trace_files():
    """Creates dummy trace files for the producers to read."""
    print("[Main] Generating dummy trace files...")
    for i in range(NUM_PRODUCERS):
        filename = TRACE_FILES[i]
        with open(filename, 'w') as f:
            # Generate a random number of data points for each file
            num_values = random.randint(200, 500) * 2 # must be even
            data = [str(random.randint(50, 1000)) for _ in range(num_values)]
            f.write(",".join(data))
    print("[Main] Dummy trace files generated.")

def cleanup_generated_files():
    """Removes the generated trace files and buffer directory."""
    print("[Main] Cleaning up generated files...")
    for filename in TRACE_FILES:
        if os.path.exists(filename):
            os.remove(filename)
    if os.path.exists(BUFFER_DIR):
        for buffer_file in os.listdir(BUFFER_DIR):
            os.remove(os.path.join(BUFFER_DIR, buffer_file))
        os.rmdir(BUFFER_DIR)
    print("[Main] Cleanup complete.")


if __name__ == "__main__":
    # Generate files needed for the simulation
    #generate_dummy_trace_files()
    
    # --- Start Consumer (Type B) ---
    consumer_server = ConsumerServer(HOST, PORT)
    consumer_thread = threading.Thread(target=consumer_server.start)
    consumer_thread.start()

    # Give the server a moment to start up
    time.sleep(1)

    # --- Start Producers (Type A) ---
    producer_threads = []
    for i in range(NUM_PRODUCERS):
        thread = threading.Thread(target=producer_thread_func, args=(i,))
        producer_threads.append(thread)
        thread.start()

    # --- Wait for all threads to complete ---
    for thread in producer_threads:
        thread.join()
    
    consumer_thread.join()

    print("\n[Main] All threads have completed their execution.")

    # Clean up the simulation files
    #cleanup_generated_files()