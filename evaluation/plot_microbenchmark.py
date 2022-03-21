"""
Read log files and plot starts

Arguments:
1: Directory of logfiles
2: Num agents

Usage:
python plot_breakdown.py <log directory>
"""
import codecs
import os
import sys
import matplotlib.pyplot as plt
import numpy as np


def read_logs(log_file, search_text):
    """Matches `search_text` within the Text param of the log.
    Returns a list of values on match from the entire log file. 

    Log file format:
    Timestamp Location Severity Text Value

    Search Text should be in lowercase
    """
    values = []
    appl_values = {}
    with codecs.open(log_file, encoding="unicode_escape") as f:
        while (line := f.readline().rstrip()):
            log_line = line.split()
            # Extract text
            text = ' '.join(log_line[3:-1])

            if search_text in text.lower():
                if log_line[-1] == '-nan' or log_line[-1] == 'nan':
                    continue

                values.append(float(log_line[-1]))

                try:
                    appl = int(log_line[-2])
                    if appl in appl_values:
                        appl_values[appl].append(float(log_line[-1]))
                    else:
                        appl_values[appl] = [float(log_line[-1])]
                except:
                    continue

    return values, appl_values


# Read the controller log to get the container->appl map
logfile_dir = sys.argv[1]
num_agents = 19
log_file = os.path.join(logfile_dir, "logfileC")

controller_search_text = ['warm start time', 'dedup start time']
print_str = []
for s in controller_search_text:
    _, appl_values = read_logs(log_file, s)
    for a in sorted(appl_values.keys()):
        avg = np.average(appl_values[a])
        if s == "cold start time":
            print_str.append(avg)
        print(s, a, avg)

plain_search = ['same func pages', 'diff func pages']
for s in plain_search:
    v, _ = read_logs(log_file, s)
    print(s, np.average(v))

_, appl_to_cont = read_logs(log_file, "assigned new container")
cont_to_appl = {}
for a in appl_to_cont:
    for c in appl_to_cont[a]:
        cont_to_appl[int(c)] = a

# Now read the agent files to get application wise breakdown
appl_search_text = ["average delta size", "dedup benefit"]
for s in appl_search_text:
    total_appl_values = {}
    for i in range(num_agents):
        log_file = os.path.join(logfile_dir, "logfile" + str(i))
        _, appl_values = read_logs(log_file, s)
        for a in appl_values:
            if a in total_appl_values:
                total_appl_values[a].extend(appl_values[a])
            else:
                total_appl_values[a] = appl_values[a]

    for a in sorted(total_appl_values.keys()):
        print(s, a, np.average(total_appl_values[a]))

cont_search_text = [
    "ml decode time", "base script time", "dedup script time",
    "restore script time", "ml restore time", "ml dedup time",
    "get base containers", "ml encode time"
]
dst = {}
for a in range(10):
    dst[a] = [0, 0, 0]
for s in cont_search_text:
    total_appl_values = {}
    for i in range(num_agents):
        log_file = os.path.join(logfile_dir, "logfile" + str(i))
        _, appl_values = read_logs(log_file, s)
        for c in appl_values:
            if c not in cont_to_appl:
                continue
            a = cont_to_appl[c]
            if a in total_appl_values:
                total_appl_values[a].extend(appl_values[c])
            else:
                total_appl_values[a] = appl_values[c]

    for a in sorted(total_appl_values.keys()):
        avg = np.average(total_appl_values[a])
        if s == "ml decode time":
            dst[a][1] = avg
        if s == "ml restore time":
            dst[a][0] = avg - dst[a][1]
        if s == "restore script time":
            dst[a][2] = avg
        print(s, a, avg)

print(print_str)
d = []
for a in sorted(dst.keys()):
    d.append(dst[a])
print(d)
