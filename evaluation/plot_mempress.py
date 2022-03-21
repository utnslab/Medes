"""
Read agent log files and controller log file to plot the slo violations and number of cold starts in
a single comparison plot.

Arguments:
2: Directory of logfiles
3: Directory of logfiles for second policy
4: Directory of logfiles for third policy
4: Append to the end of the Medes label

Usage:
python plot_comparison.py MC <Log directory 1> <Log directory 2>
"""
import os
import sys
import datetime
import numpy as np
import matplotlib.pyplot as plt

MIN_REQS = 0
MAX_REQS = 150000
PLOT_APPL = False

plt.rcParams['text.usetex'] = True  #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [
    r'\usepackage{sansmath}', r'\sansmath'
]  #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif'  # ... for regular text
plt.rcParams[
    'font.sans-serif'] = 'Computer Modern Sans serif'  # Choose a nice font here


def read_logs(log_file, search_text):
    """Matches `search_text` within the Text param of the log.
    Returns a list of values on match from the entire log file. 

    Log file format:
    Timestamp Location Severity Text Value

    Search Text should be in lowercase
    """
    values = []
    appl_values = {}
    start = False
    with open(log_file, "r") as f:
        while (line := f.readline().rstrip()):
            log_line = line.split()
            # Extract text
            text = ' '.join(log_line[3:-1])

            completed_text = 'completed reqs'
            if completed_text in text.lower():
                if not start and int(log_line[-1]) >= MIN_REQS:
                    start = True
                if int(log_line[-1]) >= MAX_REQS:
                    break

            if not start:
                continue

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
    cold_starts = 0
    start = False

    with open(log_file, "r") as f:
        while (line := f.readline().rstrip()):
            log_line = line.split()

            # Extract text
            text = ' '.join(log_line[3:-1])

            completed_text = 'completed reqs'
            if completed_text in text.lower():
                if not start and int(log_line[-1]) >= MIN_REQS:
                    start = True
                if int(log_line[-1]) >= MAX_REQS:
                    break

            if not start:
                continue

            search_text = "assigned new container"
            if search_text in text.lower():
                cold_starts += 1

    print('Cold starts: ' + str(cold_starts))
    return cold_starts


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
    return appl_coldstarts

    # slo = {1: 500, 2: 500, 3: 1000, 4: 1000, 5: 1000}
    # num_violations = 0
    # for appl in appl_startups:
    #     num = sum(s > slo[appl] for s in appl_startups[appl])
    #     num_violations += num

    # return num_violations


names = ['Medes', 'Fixed Keep-Alive', 'Adaptive Keep-Alive']
press = ['np', 'lp', 'ep']
appls = [3, 4, 5]

# Update logfile directories
logfiles = {
    'np': {
        'fixedka': '../../Analysis/experiments/api/none/15-02A',
        'adaptka': '../../Analysis/experiments/api/none/15-02B',
        'medes': '../../Analysis/experiments/api/heuristic/15-02A',
    },
    'lp': {
        'fixedka': '../../Analysis/experiments/api/none/15-02C',
        'adaptka': '../../Analysis/experiments/api/none/15-02D',
        'medes': '../../Analysis/experiments/api/heuristic/15-02Bre',
    },
    'ep': {
        'fixedka': '../../Analysis/experiments/api/none/15-02E',
        'adaptka': '../../Analysis/experiments/api/none/15-02F',
        'medes': '../../Analysis/experiments/api/heuristic/15-02C',
    },
}
base_logfile_dir = logfiles['ep']['medes']

# mapping from appl number obtained from read_logs to the name
press_names = {
    'np': "40G",
    'lp': "30G",
    'ep': "20G",
}

results = {}
for appl in appls:
    results[appl] = {'fixedka': [], 'medes': [], 'adaptiveka': []}

cum_results = {'fixedka': [], 'medes': [], 'adaptiveka': []}

for p in press:
    cum_medes = num_cold_starts(os.path.join(logfiles[p]['medes'], "logfileC"))
    cum_fixedka = num_cold_starts(
        os.path.join(logfiles[p]['fixedka'], "logfileC"))
    cum_adaptiveka = num_cold_starts(
        os.path.join(logfiles[p]['adaptka'], "logfileC"))
    cum_results['fixedka'].append(cum_fixedka)
    cum_results['medes'].append(cum_medes)
    cum_results['adaptiveka'].append(cum_adaptiveka)

    if not PLOT_APPL:
        continue

    medes = num_cold_start_per_appl(
        os.path.join(logfiles[p]['medes'], "logfileC"))
    fixedka = num_cold_start_per_appl(
        os.path.join(logfiles[p]['fixedka'], "logfileC"))
    adaptiveka = num_cold_start_per_appl(
        os.path.join(logfiles[p]['adaptka'], "logfileC"))

    for appl in appls:
        results[appl]['fixedka'].append(fixedka[appl])
        results[appl]['medes'].append(medes[appl])
        results[appl]['adaptiveka'].append(adaptiveka[appl])

xs = np.arange(3)
width = 0.2
y_upperbound = {
    3: 3000,
    4: 3000,
    5: 3000
}  # @Divyanshu update this y-axis range as you see fit
y_num_ticks = {
    3: 5,
    4: 5,
    5: 5
}  # how many ticks between 0 and y_upperbound (inclusive)are labeled

# fig = plt.figure(figsize=(7.2, 4))  # 6.4:4.8
fig = plt.figure(figsize=(10, 4))  # 6.4:4.8
ax = fig.add_subplot(111)

print(cum_results['fixedka'])
ax.bar(xs - width,
       cum_results['fixedka'],
       width=width,
       edgecolor='black',
       hatch='/',
       label=names[1])
print(cum_results['adaptiveka'])
ax.bar(xs,
       cum_results['adaptiveka'],
       width=width,
       edgecolor='black',
       hatch='.',
       label=names[2])
print(cum_results['medes'])
ax.bar(xs + width,
       cum_results['medes'],
       width=width,
       edgecolor='black',
       hatch='+',
       label=names[0])

ax.legend(loc="lower center",
          ncol=3,
          prop={
              'weight': 'bold',
              'size': 16
          },
          bbox_to_anchor=(0.5, 0.99),
          frameon=False,
          columnspacing=1)

ax.set_xticks([i for i in range(len(xs))])
ax.set_xticklabels([press_names[i] for i in press],
                   fontsize=18,
                   fontweight='bold')

yticks = np.linspace(0, 3000, 4)
ax.set_yticks(yticks)
ax.set_yticklabels([str(int(i)) for i in yticks],
                   fontsize=18,
                   fontweight='bold')

ax.grid(True, axis='y')
ax.set_xlabel('Cluster Pool Size', fontsize=18, fontweight='bold')
ax.set_ylabel('Number of cold starts', fontsize=18, fontweight='bold')

plt.subplots_adjust(bottom=0.15, top=0.85, left=0.15, right=0.9)
plt.savefig(os.path.join(base_logfile_dir, 'comparison_mempress.pdf'))

if not PLOT_APPL:
    sys.exit(0)

for appl in appls:

    fig = plt.figure(figsize=(6, 3))  # 6.4:4.8
    ax = fig.add_subplot(111)

    print(results[appl]['fixedka'])
    ax.bar(xs - width,
           results[appl]['fixedka'],
           width=width,
           edgecolor='black',
           hatch='/',
           label=names[1])
    print(results[appl]['adaptiveka'])
    ax.bar(xs,
           results[appl]['adaptiveka'],
           width=width,
           edgecolor='black',
           hatch='.',
           label=names[2])
    print(results[appl]['medes'])
    ax.bar(xs + width,
           results[appl]['medes'],
           width=width,
           edgecolor='black',
           hatch='+',
           label=names[0])

    ax.legend(loc="lower center",
              ncol=3,
              prop={
                  'weight': 'bold',
                  'size': 18
              },
              bbox_to_anchor=(0.5, 1),
              frameon=False)

    ax.set_xticks([i for i in range(len(xs))])
    ax.set_xticklabels([press_names[i] for i in press],
                       fontsize=18,
                       fontweight='bold')

    yticks = np.linspace(0, y_upperbound[appl], y_num_ticks[appl])
    ax.set_yticks(yticks)
    ax.set_yticklabels([str(int(i)) for i in yticks],
                       fontsize=18,
                       fontweight='bold')

    ax.grid(True, axis='y')
    ax.set_xlabel('Cluster Pool Size', fontsize=18, fontweight='bold')
    ax.set_ylabel('Number of cold starts', fontsize=18, fontweight='bold')

    plt.subplots_adjust(bottom=0.15, top=0.96, left=0.15, right=0.98)
    plt.savefig(
        os.path.join(base_logfile_dir, 'comparison_{0}.pdf'.format(appl)))
