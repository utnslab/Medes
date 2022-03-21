"""
Read agent log files and controller log file to plot the slo violations and number of cold starts in
a single comparison plot.

Arguments:
1: Directory of logfiles
2: Directory of logfiles for second policy
3: Directory of logfiles for third policy

Usage:
python plot_comparison.py <Log directory 1> <Log directory 2>
"""
import os
from re import A
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter
from utils import *

MIN_REQS = 0
MAX_REQS = 1500000

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
    appl_values = None
    with open(log_file, "r") as f:
        while (line := f.readline().rstrip()):
            log_line = line.split()
            # Extract text
            text = ' '.join(log_line[3:-1])

            completed_text = 'completed reqs'
            if completed_text in text.lower():
                if float(log_line[-1]) > MIN_REQS and appl_values is None:
                    appl_values = {}
                if int(log_line[-1]) >= MAX_REQS:
                    break

            if search_text in text.lower():
                if float(log_line[-1]) > MAX_REQS:
                    continue

                values.append(float(log_line[-1]))
                appl = int(log_line[-2])
                if appl_values is not None:
                    if appl in appl_values:
                        appl_values[appl].append(float(log_line[-1]))
                    else:
                        appl_values[appl] = [float(log_line[-1])]

    return values, appl_values


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


def num_cold_start_per_appl(log_file):
    _, appl_values = read_logs(log_file, "cold start time")
    appl_startups = {}
    for appl in appl_values:
        if appl not in appl_startups:
            appl_startups[appl] = appl_values[appl]
        else:
            appl_startups[appl].extend(appl_values[appl])

    appl_coldstarts = {}
    for appl in appl_startups:
        appl_coldstarts[appl] = len(appl_startups[appl])
        print('app', appl, 'has', appl_coldstarts[appl])
    for appl in appl_names:
        if appl not in appl_coldstarts:
            appl_coldstarts[appl] = 0
    return appl_coldstarts


EVAL_ADAPTIVE = len(sys.argv) == 4

if len(sys.argv) == 4:
    NAMES = ['Medes', 'Fixed Keep-Alive', 'Adaptive Keep-Alive']
    results = {'fixedka': [], 'medes': [], 'adaptiveka': []}
elif len(sys.argv) == 3:
    NAMES = ['Medes', 'Fixed Keep-Alive']
    results = {'fixedka': [], 'medes': []}

LOGFILES = []
for i in range(1, len(sys.argv)):
    # The directory with all the LOGFILES
    logfile_dir = sys.argv[i]
    logfile = os.path.join(logfile_dir, "logfileC")
    LOGFILES.append(logfile)
base_logfile_dir = sys.argv[1]

medes = num_cold_start_per_appl(LOGFILES[0])
fixedka = num_cold_start_per_appl(LOGFILES[1])
if EVAL_ADAPTIVE:
    adaptiveka = num_cold_start_per_appl(LOGFILES[2])

appl_order = []
if len(fixedka) != len(medes):
    print("MAYBE AN ERROR")
sorted_keys = sorted(appl_names.items(), key=lambda i: i[0])
for appl in sorted_keys:
    print(appl, fixedka[appl[0]], medes[appl[0]])
    # if medes[appl[0]] > fixedka[appl[0]]:
    #     medes[appl[0]] = 0.9 * fixedka[appl[0]]
    results['fixedka'].append(fixedka[appl[0]])
    results['medes'].append(medes[appl[0]])
    print(fixedka[appl[0]] / medes[appl[0]],
          adaptiveka[appl[0]] / medes[appl[0]])
    if EVAL_ADAPTIVE:
        results['adaptiveka'].append(adaptiveka[appl[0]])
    appl_order.append(appl[0])

# policies = []
# for logfile in logfiles:
#     num1 = num_cold_starts(logfile)
#     num2 = slo_violations(logfile)
#     policies.append([num1, num2])

xs = np.arange(len(fixedka))
width = 0.24

y_upperbound = 800  # update this y-axis range as you see fit
y_num_ticks = 9  # how many ticks between 0 and y_upperbound (inclusive)are labeled

fig = plt.figure(figsize=(7.2, 6.4))  # 6.4:4.8
ax = fig.add_subplot(111)

if EVAL_ADAPTIVE:
    print(results['fixedka'])
    ax.barh(xs - width,
           results['fixedka'],
           height=width,
           edgecolor='black',
           hatch='//',
           label=NAMES[1])
    print(results['adaptiveka'])
    ax.barh(xs,
           results['adaptiveka'],
           height=width,
           edgecolor='black',
           hatch='|',
           label=NAMES[2])
    print(results['medes'])
    ax.barh(xs + width,
           results['medes'],
           height=width,
           edgecolor='black',
           hatch='x',
           label=NAMES[0])
else:
    print(results['fixedka'])
    ax.bar(xs - width / 2,
           results['fixedka'],
           width=width,
           edgecolor='black',
           hatch='//',
           label=NAMES[1])
    print(results['medes'])
    ax.bar(xs + width / 2,
           results['medes'],
           width=width,
           edgecolor='black',
           hatch='+',
           label=NAMES[0])

ax.legend(loc="upper right", ncol=1, prop={'weight': 'bold', 'size': 14})

ax.set_yticks(xs)
ax.set_yticklabels([appl_names[i] for i in appl_order],
                   fontsize=12,
                   fontweight='bold',
                   rotation=22.5)
ax.invert_yaxis()

yticks = np.linspace(0, y_upperbound, y_num_ticks)
# yticks = [50, 100, 200, 400, 800]
# ax.set_xscale('log')
ax.set_xticks(yticks)
ax.set_xticklabels([str(int(i)) for i in yticks], fontsize=14, fontweight='bold')
# plt.tick_params(axis='x', which='minor')
# ax.tick_params(which='minor', labelsize=12)
# ax.xaxis.set_minor_formatter(FormatStrFormatter("%.0f"))
ax.xaxis.set_major_formatter(ScalarFormatter())

ax.grid(True, axis='x')
# ax.set_ylabel('Function', fontsize=16, fontweight='bold')
ax.set_xlabel('Number of cold starts', fontsize=16, fontweight='bold')

plt.subplots_adjust(bottom=0.1, top=0.96, left=0.15, right=0.97)
plt.savefig(os.path.join(base_logfile_dir, 'comparison.pdf'))