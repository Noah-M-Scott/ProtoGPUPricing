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

# --- Producer (Type A) Constants ---
NUM_PRODUCERS = 4  # The number of producer threads to create.
NODE_NUMEBER = 0   # Index of the current node

# Array of trace file names. Must have at least NUM_PRODUCERS elements.
TRACE_FILES = [f"traces/trace_{i}.trace" for i in range(NUM_PRODUCERS)]
N_MICROSECONDS = 10    # Interval to read from trace file (This is swapped out for a file defined per line latency)
M_MICROSECONDS = 50    # Interval to process data and add to log
K_ITEMS = 131072       # Number of items in the temporary list before sending.
RUN_TIMES = 3          # Number of times to rerun the trace

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
    thread_name = f"Producer {index + NODE_NUMEBER}"

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
        bandwidth, compute, latency, packet_counter, runs = 0, 0, 0, 0, 0
        
        last_read_time = time.perf_counter_ns()
        last_process_time = time.perf_counter_ns()

        # --- Main loop ---
        while True:
            current_time = time.perf_counter_ns()
            
            # --- Task 1: Read from trace file every N microseconds (or according to a per file per line latency) ---
            if (current_time - last_read_time) / 1000 >= latency:
            #if int(current_time - last_read_time) >= N_MICROSECONDS:
                try:
                    latency = int(next(values))
                    compute = int(float(next(values)) * 100)
                    bandwidth = int(float(next(values)) * 100)
                    #print(f"[{thread_name}] Read: lt={latency}, bw={bandwidth}, comp={compute}") # Uncomment for verbose logging
                    last_read_time = current_time
                except StopIteration:
                    # End of file reached
                    if runs < RUN_TIMES :
                        with open(trace_file_name, 'r') as f:
                            values = iter(f.read().split(','))
                        runs += 1
                        print(f"[{thread_name}] Reached end of trace file.")
                    else:
                        print(f"[{thread_name}] Has finished runs.")
                        break # Exit the main while loop
            
            
            # --- Task 2: Process and potentially send data every M microseconds ---
            if (current_time - last_process_time) / 1000 >= M_MICROSECONDS:
                # Accumulate amount owed
                amount_owed += pricing_function(bandwidth, compute)
                # Add to temporary list
                temp_list.append((bandwidth, compute))
                
                last_process_time = current_time
                
                # Check if the list is ready to be sent
                if len(temp_list) >= K_ITEMS:
                    payload = {
                        "index": index + NODE_NUMEBER,
                        "packet": packet_counter,
                        "amountOwed": amount_owed,
                        "data": temp_list
                    }
                    serialized_payload = pickle.dumps(payload)
                    
                    data_length = len(serialized_payload)
                    
                    length_header = struct.pack("!I", data_length) 
                    
                    
                    print(f"[{thread_name}] Sending batch of {len(temp_list)} items.")
                    client_socket.sendall(length_header + serialized_payload)
                    
                    # Reset the temporary list for the next batch
                    temp_list = []
                    
                    #next packet
                    packet_counter += 1
            
            
        
        # --- Termination ---
        # Send any remaining data before closing
        if temp_list:
            payload = {
                "index": index + NODE_NUMEBER,
                "packet":packet_counter,
                "amountOwed": amount_owed,
                "data": temp_list
            }
            serialized_payload = pickle.dumps(payload)
            
            data_length = len(serialized_payload)
            
            length_header = struct.pack("!I", data_length) 
            
            print(f"[{thread_name}] Sending final batch of {len(temp_list)} items.")
            client_socket.sendall(length_header + serialized_payload)
            
        # Send the "I'm done" message
        print(f"[{thread_name}] Sending 'I'm done' message and shutting down.")
        done_message = pickle.dumps(f"DONE:{index}")
        
        data_length = len(done_message)
        length_header = struct.pack("!I", data_length) 
        
        client_socket.sendall(length_header + done_message)
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
