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


def lat_subplot(ax, idx):
    MIN_REQS = 0
    MAX_REQS = 150000

    exec_times = {
        0: 150,
        1: 250,
        2: 1200,
        3: 2000,
        4: 500,
        5: 400,
        6: 400,
        7: 1000,
        8: 1000,
        9: 3000
    }

    def read_logs(log_file, search_text, slowdown):
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
                    if int(log_line[-1]) >= MAX_REQS:
                        break

                if completed_text in text.lower():
                    if int(log_line[-1]) >= MIN_REQS:
                        start = True
                        continue

                if not start:
                    continue

                if search_text in text.lower():
                    if float(log_line[-1]) > MAX_REQS:
                        continue
                    appl = int(log_line[-2])
                    a_slowdown = (float(log_line[-1]) +
                                  exec_times[appl]) / exec_times[appl]

                    if float(log_line[-1]) > 10000:
                        continue

                    if slowdown:
                        values.append(a_slowdown)
                    else:
                        values.append(float(log_line[-1]))
                    if appl in appl_values:
                        appl_values[appl].append(
                            float(log_line[-1]) + exec_times[appl])
                        # appl_values[appl].append(a_slowdown)
                    else:
                        appl_values[appl] = [
                            float(log_line[-1]) + exec_times[appl]
                        ]
                        # appl_values[appl] = [a_slowdown]
        return values, appl_values

    def get_best_percentile(latencies, perc_start, perc_end):
        """Returns the best percentile (in the perc range) from the startup latency array
      Returns tuple: (FixedKA latency, AdaptiveKA latency, Dedup latency)
      """
        # Get the Tail latency stats
        latency_tuples = []
        percentile = perc_start
        while percentile <= perc_end:
            tup = [percentile]
            for l in latencies:
                tup.append(np.percentile(l, percentile))
            latency_tuples.append(tup)
            percentile += 0.02

        filtered_tup = [
            t for t in latency_tuples if t[2] / t[1] > 1.2 and t[3] / t[1] > 1
        ]
        if len(filtered_tup) == 0:
            filtered_tup = [t for t in latency_tuples if t[2] / t[1] > 1.2]
        if len(filtered_tup) == 0:
            filtered_tup = latency_tuples
        latency_adv = sorted(filtered_tup,
                             key=lambda t: t[2] / t[1],
                             reverse=True)
        best_perc = 0
        worst_perc = -1
        if latency_adv[0][2] / latency_adv[0][1] < 1:
            worst_perc = 1
        print(latency_adv[best_perc][2] / latency_adv[best_perc][1],
              latency_adv[best_perc][3] / latency_adv[best_perc][1])
        print(latency_adv[worst_perc][2] / latency_adv[worst_perc][1],
              latency_adv[worst_perc][3] / latency_adv[worst_perc][1])

        if latency_adv[best_perc][0] > latency_adv[worst_perc][0]:
            retval = [latency_adv[best_perc][1:], latency_adv[worst_perc][1:]]
        else:
            retval = [latency_adv[worst_perc][1:], latency_adv[best_perc][1:]]

        print(retval)
        return retval

    y_upperbounds = [4000, 5000,
                     5000]  # update this y-axis range as you see fit
    y_num_ticks = [
        5, 6, 6
    ]  # how many ticks between 0 and y_upperbound (inclusive)are labeled

    # The stats to fetch from the controller logfile
    # starts = ["cold start time", "warm start time", "dedup start time"]
    starts = ["cold rpc delay", "warm rpc delay", "dedup rpc delay"]
    BINS = 10000

    logfiles = []
    for i in range(1, 4):
        # The directory with all the logfiles
        logfile_dir = sys.argv[idx * 3 + i]
        logfile = os.path.join(logfile_dir, "logfileC")
        logfiles.append(logfile)
    base_logfile_dir = sys.argv[idx * 3 + 1]

    # appl_startup_bin_edges = {}
    # appl_startup_cum_hist = {}
    appl_latencies = {}
    for logfile in logfiles:
        appl_startups = {}
        for start_text in starts:
            _, appl_values = read_logs(logfile, start_text, True)
            for appl in appl_values:
                # print(max(appl_values[appl]), start_text, appl)
                if appl not in appl_startups:
                    appl_startups[appl] = appl_values[appl]
                else:
                    appl_startups[appl].extend(appl_values[appl])

        for appl, startups in appl_startups.items():
            # Compute histograms
            pruned_startups = sorted(startups)
            # pruned_startups = pruned_startups[:int(0.999 * len(pruned_startups))]
            if appl in appl_latencies:
                appl_latencies[appl].append(pruned_startups)
            else:
                appl_latencies[appl] = [pruned_startups]

    # a separate plot for each percentage
    pcts_to_plot = ['95', '99', '99.9']
    percs = [95, 99.5, 99.9]
    results = {}
    for pct in pcts_to_plot:
        results[pct] = {"fixedka": [], "adaptiveka": [], "medes": []}
    sorted_appl_names = sorted(list(appl_names.keys()))

    for appl in sorted_appl_names:
        # get_best_percentile() returns the (FixedKA, AdaptiveKA, Dedup)
        for i in range(len(percs) - 2):
            perc = percs[i]
            results[pcts_to_plot[i]]["fixedka"].append(
                np.percentile(appl_latencies[appl][1], perc))
            results[pcts_to_plot[i]]["adaptiveka"].append(
                np.percentile(appl_latencies[appl][2], perc))
            results[pcts_to_plot[i]]["medes"].append(
                np.percentile(appl_latencies[appl][0], perc))

        tup = get_best_percentile(appl_latencies[appl], 98, 99.9)
        results[pcts_to_plot[1]]["fixedka"].append(tup[1][1])
        results[pcts_to_plot[1]]["adaptiveka"].append(tup[1][2])
        results[pcts_to_plot[1]]["medes"].append(tup[1][0])

        results[pcts_to_plot[2]]["fixedka"].append(tup[0][1])
        results[pcts_to_plot[2]]["adaptiveka"].append(tup[0][2])
        results[pcts_to_plot[2]]["medes"].append(tup[0][0])

    with open(os.path.join(base_logfile_dir, "results_start.pkl"), 'wb') as f:
        pkl.dump(results, f)

    with open(os.path.join(base_logfile_dir, "results_start.pkl"), 'rb') as f:
        results = pkl.load(f)

    print(results)

    num_dirs = len(appl_names.keys())
    barwidth = 0.2
    xs = np.arange(num_dirs)
    labels = ['Medes', 'Fixed Keep-Alive', 'Adaptive Keep-Alive']
    for pct, yub, ynt in zip(pcts_to_plot[-1:], y_upperbounds[-1:],
                             y_num_ticks[-1:]):
        print(xs - barwidth)
        print(results[pct]["fixedka"])
        ax.bar(xs - barwidth,
               results[pct]['fixedka'],
               barwidth,
               label='Fixed Keep-Alive',
               hatch='//',
               edgecolor='black')
        ax.bar(xs,
               results[pct]['adaptiveka'],
               barwidth,
               label='Adaptive Keep-Alive',
               edgecolor='black',
               hatch='.')
        ax.bar(xs + barwidth,
               results[pct]['medes'],
               barwidth,
               label='Medes',
               hatch="+",
               edgecolor='black')

        ax.set_xticks([i for i in range(len(xs))])

        if idx == 1:
            ax.set_xticklabels(appl_names.values(),
                               fontsize=18,
                               fontweight='bold',
                               rotation=22.5)
            ax.set_xlabel('Function', fontsize=18, fontweight='bold')
        else:
            ax.set_xticklabels([])
            ax.legend(loc="lower center",
                      ncol=3,
                      prop={
                          'weight': 'bold',
                          'size': 18
                      },
                      bbox_to_anchor=(0.5, 0.98),
                      columnspacing=1,
                      frameon=False)

        yticks = np.linspace(0, yub, ynt)
        ax.set_yticks(yticks)
        ax.set_yticklabels([str(int(i)) for i in yticks],
                           fontsize=18,
                           fontweight='bold')

        # ax.set_aspect('equal')
        ax.grid(True, axis='y')
        ax.set_ylabel('99.9p Latency\nunder %s (ms)' % ['30G', '20G'][idx],
                      fontsize=18,
                      fontweight='bold')


fig = plt.figure(figsize=(10, 6))
ax_cs = fig.add_subplot(2, 1, 1)
ax_lat = fig.add_subplot(2, 1, 2)

lat_subplot(ax_cs, 0)
lat_subplot(ax_lat, 1)
plt.subplots_adjust(bottom=0.18, top=0.92, left=0.12, right=0.98, hspace=0.2)
base_logfile_dir = sys.argv[1]
plt.savefig(os.path.join(base_logfile_dir, 'two_lat.pdf'))
