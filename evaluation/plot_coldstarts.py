"""
Read agent log files and controller log file to plot the slo violations and number of cold starts in
a single comparison plot.

Arguments: Directories of logfiles

Usage:
python plot_coldstarts.py <Log directory 1> ... <Log directory N>
"""
import os
import sys
import datetime
import numpy as np
import matplotlib.pyplot as plt

MAX_REQS = 239889

plt.rcParams['text.usetex'] = True #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [r'\usepackage{sansmath}', r'\sansmath'] #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif' # ... for regular text
plt.rcParams['font.sans-serif'] = 'Computer Modern Sans serif' # Choose a nice font here

def read_logs(log_file, search_text):
    """Matches `search_text` within the Text param of the log.
    Returns a list of values on match from the entire log file. 

    Log file format:
    Timestamp Location Severity Text Value

    Search Text should be in lowercase
    """
    values = []
    appl_values = {}
    with open(log_file, "r") as f:
        while (line := f.readline().rstrip()):
            log_line = line.split()
            # Extract text
            text = ' '.join(log_line[3:-1])

            completed_text = 'completed reqs'
            if completed_text in text.lower():
                if int(log_line[-1]) >= MAX_REQS:
                    break

            if search_text in text.lower():
                values.append(float(log_line[-1]))
                appl = int(log_line[-2])
                if appl in appl_values:
                    appl_values[appl].append(float(log_line[-1]))
                else:
                    appl_values[appl] = [float(log_line[-1])]

    return values, appl_values


def num_cold_starts(log_file):
    """Reads the controller logs and returns the number of cold starts 

    Args:
        log_file (str) : Path to the controller log file
    """
    last_time = None
    cold_starts = 0

    with open(log_file, "r") as f:
        while (line := f.readline().rstrip()):
            log_line = line.split()
            # Extract text
            start = -1
            text = ' '.join(log_line[3:-1])

            search_text = "assigned new container"
            if search_text in text.lower():
                # Get the date and count num since last second
                date_time_str = ' '.join(log_line[0:2])[1:-1]
                date_time_obj = datetime.datetime.strptime(
                    date_time_str, '%Y-%m-%d %H:%M:%S.%f')
                start = 1  # Cold start
                cold_starts += 1

    print('Cold starts: ' + str(cold_starts))
    return cold_starts


def slo_violations(log_file):
    starts = ["cold start time", "warm start time", "dedup start time"]
    appl_startups = {}
    for start_text in starts:
        _, appl_values = read_logs(log_file, start_text)
        for appl in appl_values:
            if appl not in appl_startups:
                appl_startups[appl] = appl_values[appl]
            else:
                appl_startups[appl].extend(appl_values[appl])

    slo = {1: 500, 2: 500, 3: 1000, 4: 1000, 5: 1000}
    num_violations = 0
    for appl in appl_startups:
        num = sum(s > slo[appl] for s in appl_startups[appl])
        num_violations += num

    return num_violations


def total_num_cold_starts(log_file):
    cold_starts, _ = read_logs(log_file, "cold start time")
    return len(cold_starts)

NAMES = ['Medes', 'Fixed\nKeep-Alive', 'Adaptive\nKeep-Alive']

LOGFILES = []
for i in range(1, len(sys.argv)):
    # The directory with all the logfiles
    logfile_dir = sys.argv[i]
    logfile = os.path.join(logfile_dir, "logfileC")
    LOGFILES.append(logfile)
base_logfile_dir = sys.argv[1]

bars = []
for l in LOGFILES:
    bars.append(total_num_cold_starts(l))

xs = np.arange(1, len(LOGFILES) + 1)
width = 0.4

# a separate plot for each percentage
# pcts_to_plot = ['50', '99.9']
y_upperbound = 4000  # @Divyanshu update this y-axis range as you see fit
y_num_ticks = 6  # how many ticks between 0 and y_upperbound (inclusive)are labeled

# print(policies)

# Grouped bar plot
# bar1 = plt.bar(ind, [policies[0][0]], width, color='r')
# bar2 = plt.bar(ind + width, [policies[1][0]], width, color='g')

# plt.ylabel("Number")
# plt.xticks(ind, ['Cold Starts', 'SLO Violations'])
# plt.legend((bar1, bar2), ('Heuristic', 'None'))
# plt.savefig(os.path.join(base_logfile_dir, 'comparison.png'))

# @TAO: HERE
# cold_starts = [20 * policies[0][0], 20 * policies[1][0]]

fig = plt.figure(figsize=(4, 3.6))  # 6.4:4.8
ax = fig.add_subplot(111)

ax.bar(xs, bars, width=width, edgecolor='black')
print(bars)

# ax.legend(loc="upper left", ncol=1, prop={'weight': 'bold', 'size': 14})

ax.set_xticks(np.arange(1, len(xs) + 1))
ax.set_xticklabels(NAMES, fontsize=14, fontweight='bold')
ax.set_xlim(0.75, len(xs) + 0.25)

yticks = np.linspace(0, y_upperbound, y_num_ticks)
ax.set_yticks(yticks)
ax.set_yticklabels([str(int(i)) for i in yticks],
                   fontsize=12,
                   fontweight='bold')

ax.grid(True, axis='y')
ax.set_xlabel('Policy', fontsize=16, fontweight='bold')
ax.set_ylabel('Number of cold starts', fontsize=16, fontweight='bold')

plt.subplots_adjust(bottom=0.15, top=0.96, left=0.2, right=0.95)
plt.savefig(os.path.join(base_logfile_dir, 'cold_starts.pdf'))