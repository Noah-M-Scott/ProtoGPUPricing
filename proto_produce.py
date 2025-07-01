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
M_MICROSECONDS = 500    # Interval to process data and add to log (5000 us = 5 ms).
K_ITEMS = 128          # Number of items in the temporary list before sending.

# ==============================================================================
#  Pricing Function
# ==============================================================================

def pricing_function(bandwidth: int, compute: int) -> float:
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
    
    # --- Start Producers (Type A) ---
    producer_threads = []
    for i in range(NUM_PRODUCERS):
        thread = threading.Thread(target=producer_thread_func, args=(i,))
        producer_threads.append(thread)
        thread.start()

    # --- Wait for all threads to complete ---
    for thread in producer_threads:
        thread.join()
    

    print("\n[Main] All threads have completed their execution.")

    # Clean up the simulation files
    #cleanup_generated_files()
