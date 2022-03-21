import os
import sys
import pickle as pkl
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
from utils import *

plt.rcParams['text.usetex'] = True  #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [
    r'\usepackage{sansmath}', r'\sansmath'
]  #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif'  # ... for regular text
plt.rcParams[
    'font.sans-serif'] = 'Computer Modern Sans serif'  # Choose a nice font here


def cs_subplot(ax, idx):
    MIN_REQS = 0
    MAX_REQS = 150000

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

    # policy = 'Memory' if sys.argv[1] == 'MC' else 'Latency'
    # append = '' if sys.argv[5] == 'NONE' else sys.argv[5]
    eval_adaptive = True

    names = ['Medes', 'Fixed Keep-Alive', 'Adaptive Keep-Alive']
    results = {'fixedka': [], 'medes': [], 'adaptiveka': []}

    logfiles = []
    for i in range(1, 4):
        # The directory with all the logfiles
        logfile_dir = sys.argv[idx * 3 + i]
        logfile = os.path.join(logfile_dir, "logfileC")
        logfiles.append(logfile)

    medes = num_cold_start_per_appl(logfiles[0])
    fixedka = num_cold_start_per_appl(logfiles[1])
    if eval_adaptive:
        adaptiveka = num_cold_start_per_appl(logfiles[2])

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
        if eval_adaptive:
            results['adaptiveka'].append(adaptiveka[appl[0]])
        appl_order.append(appl[0])

    xs = np.arange(len(fixedka))
    width = 0.24

    y_upperbound = 800  # update this y-axis range as you see fit
    y_num_ticks = 5  # how many ticks between 0 and y_upperbound (inclusive)are labeled

    if eval_adaptive:
        print(results['fixedka'])
        ax.bar(xs - width,
               results['fixedka'],
               width=width,
               edgecolor='black',
               hatch='//',
               label=names[1])
        print(results['adaptiveka'])
        ax.bar(xs,
               results['adaptiveka'],
               width=width,
               edgecolor='black',
               hatch='.',
               label=names[2])
        print(results['medes'])
        ax.bar(xs + width,
               results['medes'],
               width=width,
               edgecolor='black',
               hatch='+',
               label=names[0])
    else:
        print(results['fixedka'])
        ax.bar(xs - width / 2,
               results['fixedka'],
               width=width,
               edgecolor='black',
               hatch='//',
               label=names[1])
        print(results['medes'])
        ax.bar(xs + width / 2,
               results['medes'],
               width=width,
               edgecolor='black',
               hatch='+',
               label=names[0])

    if idx == 0:
        ax.legend(loc="lower center",
                  ncol=3,
                  prop={
                      'weight': 'bold',
                      'size': 18
                  },
                  bbox_to_anchor=(0.5, 0.98),
                  columnspacing=1,
                  frameon=False)

    ax.set_xticks(xs)
    if idx == 1:
        ax.set_xticklabels(appl_names.values(),
                           fontsize=18,
                           fontweight='bold',
                           rotation=22.5)
        ax.set_xlabel('Function', fontsize=18, fontweight='bold')
    else:
        ax.set_xticklabels([])

    yticks = np.linspace(0, y_upperbound, y_num_ticks)
    ax.set_yticks(yticks)
    ax.set_yticklabels([str(int(i)) for i in yticks],
                       fontsize=18,
                       fontweight='bold')
    
    ax.grid(True, axis='y')
    ax.set_ylabel('No. of cold starts\nunder %s' % ['30G', '20G'][idx],
                  fontsize=18,
                  fontweight='bold')


fig = plt.figure(figsize=(10, 6))
ax_cs = fig.add_subplot(2, 1, 1)
ax_cs_1 = fig.add_subplot(2, 1, 2)

cs_subplot(ax_cs, 0)
cs_subplot(ax_cs_1, 1)

plt.subplots_adjust(bottom=0.18, top=0.92, left=0.12, right=0.98, hspace=0.1)
base_logfile_dir = sys.argv[1]
plt.savefig(os.path.join(base_logfile_dir, 'two_cs.pdf'))