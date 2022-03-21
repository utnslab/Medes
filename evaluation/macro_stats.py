"""Start measuring and trap INTERRUPT signal to gracefully end execution"""
import os
import sys
import time
import datetime
import pickle
import psutil
import signal

arg = sys.argv[1]


def signal_handler(sig, frame):
    # Write the stats to a pickle file, which can be read by the plot macro script
    used_mem = [(total_memory - a) for a in available_memory]
    mem_usage = {
        'used': used_mem,
        'checkpoints': checkpoints_size,
        'appl': appl_memory,
        'cpu': cpu_usage
    }
    with open("memory_stats_{0}.pkl".format(arg), "wb") as f:
        pickle.dump(mem_usage, f)

    sys.exit(0)


def get_checkpoints_size():
    total_size = 0
    for dirpath, _, filenames in os.walk("/tmp/checkpoints"):
        # Only use the dedup container dumps
        if any(['patch-' in f for f in filenames]):
            for f in filenames:
                # Do not use the pages img file as they are always m-maped into dedup_appl process
                if 'pages-' in f:
                    continue
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if os.path.exists(fp) and not os.path.islink(fp):
                    total_size += os.path.getsize(fp)

    return total_size / (1024**2)


# Register a signal handler
signal.signal(signal.SIGINT, signal_handler)

# Hold the memory stats
total_memory = psutil.virtual_memory().total / (1024**2)  # In MB
available_memory = []
checkpoints_size = []
appl_memory = []
cpu_usage = []
breakdown = []

appl_pid = -1
last_update_time = datetime.datetime.now()

# Continuously monitor usage every second
while True:
    avbl_mem = psutil.virtual_memory().available / (1024**2)
    available_memory.append(avbl_mem)
    print(datetime.datetime.now(), avbl_mem)

    cpu = psutil.cpu_percent()
    cpu_usage.append(cpu)

    # Add the size of the checkpoints directory as well
    cp_size = get_checkpoints_size()
    checkpoints_size.append(cp_size)

    # Check if dedup_appl has been started
    if appl_pid == -1:
        for proc in psutil.process_iter():
            if 'dedup_appl' in proc.name():
                appl_pid = proc.pid
    else:
        # If it is started, log its memory usage
        try:
            process = psutil.Process(appl_pid)
            # Use the unique set size
            appl_memory.append(process.memory_full_info().uss / (1024**2))
        except:
            appl_pid = -1

    used_mem = [(total_memory - a) for a in available_memory]
    mem_usage = {
        'used': used_mem,
        'checkpoints': checkpoints_size,
        'appl': appl_memory,
        'cpu': cpu_usage
    }
    with open("memory_stats_{0}.pkl".format(arg), "wb") as f:
        pickle.dump(mem_usage, f)

    diff = datetime.datetime.now() - last_update_time
    elapsed_time = int((diff.seconds * 1000) + (diff.microseconds / 1000))
    time.sleep((1000 - elapsed_time) / 1000)
    last_update_time = datetime.datetime.now()
