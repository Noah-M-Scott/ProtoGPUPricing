import socket
import threading
import time
import pickle
import os
import random
import struct
import re
from queue import Queue
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
N_MICROSECONDS = 10    # Interval to read from trace file (This is swapped out for a file defined per line latency)
M_MICROSECONDS = 50    # Interval to process data and add to log
K_ITEMS = 131072       # Number of items in the temporary list before sending.
RUN_TIMES = 3          # Number of times to rerun the trace

# Thread Queues
THREAD_QUEUES = []
for i in range(0, NUM_PRODUCERS):
    THREAD_QUEUES.append(Queue())


# ==============================================================================
#  Pricing Function
# ==============================================================================

def pricing_function(bandwidth: int, compute: int) -> float:
    price = (bandwidth * 0.01) + (compute * 0.005)
    return price

# ==============================================================================
#  Encryption Function
# ==============================================================================

def encryption_function(packet):
    
    #encrypt
    
    return packet


# ==============================================================================
#  Producer Thread
# ==============================================================================

def get_files_in_directory(directory_path):
    """
    Returns a list of all files in the specified directory.
    """
    files = []
    for entry in os.listdir(directory_path):
        full_path = os.path.join(directory_path, entry)
        if os.path.isfile(full_path):
            files.append(full_path)
    return files


def producer_packet_manager_func(index: int):
    """
    The helper function for a producer thread.
    - Connects to the consumer's socket.
    - Recieves data packets from it's sampling twin
    - Periodically processes (encrypts) and sends data to the consumer.
    """
    
    thread_name = f"Producer {index + NODE_NUMEBER * NUM_PRODUCERS}"
    print(f"[Producer {index + NODE_NUMEBER * NUM_PRODUCERS} Manager] Starting.")
    
    
    try:
        # --- Connect to consumer socket ---
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((HOST, PORT))
        print(f"[{thread_name}] Connected to consumer at {HOST}:{PORT}")
        
        # --- State variables ---
        last_read_time = time.perf_counter_ns()
        last_process_time = time.perf_counter_ns()
        
        # --- Main loop ---
        while True:
            payload = THREAD_QUEUES[index].get()
            
            #null payload for shutdown
            if payload['index'] == -1:
                break
            
            #otherwise...
            
            #do encryption
            payload = encryption_function(payload)
            print(f"[{thread_name}] payload encrypted, took X seconds (DATA VOLUME MEASURE POINT #1)")
            
            
            #and send
            serialized_payload = pickle.dumps(payload)
            
            data_length = len(serialized_payload)
            
            length_header = struct.pack("!I", data_length) 
            
            
            print(f"[{thread_name}] Sending batch of {len(payload['data'])} items.")
            client_socket.sendall(length_header + serialized_payload)
        
        
        # Send the "I'm done" message
        print(f"[{thread_name}] Sending 'I'm done' message and shutting down.")
        done_message = pickle.dumps(f"DONE:{index}")
        
        data_length = len(done_message)
        length_header = struct.pack("!I", data_length) 
        
        client_socket.sendall(length_header + done_message)
        client_socket.close()
        print(f"[{thread_name}] Finished.")
        
        
    except ConnectionRefusedError:
        print(f"[ERROR][{thread_name}] Connection refused. Is the consumer server running?")
    
    return


def producer_thread_func(index: int):
    """
    The main function for a producer thread.
    - Reads data from its assigned trace file.
    - Periodically sends data packets to it's manager twin
    """
    
    thread_name = f"Producer {index + NODE_NUMEBER * NUM_PRODUCERS}"
    print(f"[Producer {index + NODE_NUMEBER * NUM_PRODUCERS} Sampler] Starting.")
    
    TRACE_FILES = get_files_in_directory("traces")
    
    try:
        # --- File and variable setup ---
        trace_file_name = TRACE_FILES[random.randint(0, len(TRACE_FILES) - 1)]
        with open(trace_file_name, 'r') as f:
            # Read all whitespace/'|'-separated values at once and create an iterator
            values = iter(re.split(r"[\s\|]+", f.read()))
            next(values) #burn headers
            next(values)
            next(values)
            next(values)
        print(f"[{thread_name}] trace selected is {trace_file_name}")
        
        # --- State variables ---
        temp_list = []
        amount_owed = 0.0
        bandwidth, compute, latency, packet_counter, runs = 0, 0, 0, 0, 0
        actualTime_accum, actualTime_count = 0, 0
        
        last_read_time = time.perf_counter_ns()
        last_process_time = time.perf_counter_ns()

        # --- Main loop ---
        while True:
            current_time = time.perf_counter_ns()
            
            # --- Task 1: Read from trace file every N microseconds (or according to a per file per line latency) ---
            #if int(current_time - last_read_time) >= N_MICROSECONDS:
            if (current_time - last_read_time) * 0.001 >= latency:
                
                try:
                    next(values) #burn the name
                    latency = int(next(values)) / 1000 #nano to micro
                    compute = int(float(next(values)) * 100)
                    bandwidth = int(float(next(values)) * 100)
                    #print(f"[{thread_name}] Read: lt={latency}, bw={bandwidth}, comp={compute}") # Uncomment for verbose logging
                    last_read_time = current_time
                except StopIteration:
                    # End of file reached
                    if runs < RUN_TIMES :
                        print(f"[{thread_name}] Reached end of trace file.")
                        trace_file_name = TRACE_FILES[random.randint(0, len(TRACE_FILES) - 1)]
                        with open(trace_file_name, 'r') as f:
                            # Read all comma-separated values at once and create an iterator
                            values = iter(re.split(r"[\s\|]+", f.read()))
                            next(values) #burn headers
                            next(values)
                            next(values)
                            next(values)
                        print(f"[{thread_name}] trace selected is {trace_file_name}")
                        
                        runs += 1
                    else:
                        print(f"[{thread_name}] Has finished runs.")
                        break # Exit the main while loop
            
            
            # --- Task 2: Process and potentially send data every M microseconds ---
            if (current_time - last_process_time) * 0.001 >= M_MICROSECONDS:
                
                # Test for bad slowdown
                actualTime_accum += (current_time - last_process_time) * 0.001 
                actualTime_count += 1
                
                # Accumulate amount owed
                amount_owed += pricing_function(bandwidth, compute)
                # Add to temporary list
                temp_list.append((bandwidth, compute))
                
                last_process_time = current_time
                
                # Check if the list is ready to be sent
                if len(temp_list) >= K_ITEMS:
                    payload = {
                        "index": index + NODE_NUMEBER * NUM_PRODUCERS,
                        "packet": packet_counter,
                        "amountOwed": amount_owed,
                        "data": temp_list
                    }
                    
                    #send to manager
                    THREAD_QUEUES[index].put(payload)
                    
                    # Reset the temporary list for the next batch
                    temp_list = []
                    
                    #next packet
                    packet_counter += 1
        
        
        # --- Termination ---
        # Send any remaining data before closing
        if temp_list:
            payload = {
                "index": index + NODE_NUMEBER * NUM_PRODUCERS,
                "packet":packet_counter,
                "amountOwed": amount_owed,
                "data": temp_list
            }
            
            #send to manager
            THREAD_QUEUES[index].put(payload)
        
        
        # Send finish comand to manager
        payload = { "index": -1 }
        THREAD_QUEUES[index].put(payload)
        
        print(f"[{thread_name}] Finished.")
        print(f"Average sample time was {actualTime_accum / actualTime_count}")
        
        
    except FileNotFoundError:
        print(f"[ERROR][{thread_name}] Trace file not found: {trace_file_name}")
    
    return


# ==============================================================================
#  Main Execution
# ==============================================================================

if __name__ == "__main__":
    
    # --- Start Producers ---
    producer_threads = []
    for i in range(NUM_PRODUCERS):
        
        #manager
        thread = threading.Thread(target=producer_packet_manager_func, args=(i,))
        producer_threads.append(thread)
        thread.start()
        
        #sampler
        thread = threading.Thread(target=producer_thread_func, args=(i,))
        producer_threads.append(thread)
        thread.start()
    
    
    # --- Wait for all threads to complete ---
    for thread in producer_threads:
        thread.join()
    

    print("\n[Main] All threads have completed their execution.")
    
