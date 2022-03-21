import os
import sys
import pickle as pkl
import numpy as np
import matplotlib.pyplot as plt
from utils import *

plt.rcParams['text.usetex'] = True  #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [
    r'\usepackage{sansmath}', r'\sansmath'
]  #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif'  # ... for regular text
plt.rcParams[
    'font.sans-serif'] = 'Computer Modern Sans serif'  # Choose a nice font here


def cdf_subplot(axs):

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
                    if float(log_line[-1]) > MAX_REQS:
                        continue
                    appl = int(log_line[-2])
                    values.append(float(log_line[-1]))
                    # if appl > 5:
                    #     raise Exception()
                    if appl in appl_values:
                        appl_values[appl].append(float(log_line[-1]))
                    else:
                        appl_values[appl] = [float(log_line[-1])]

            return values, appl_values

    # The stats to fetch from the controller logfile
    BINS = 10000

    logfiles = []
    for i in range(1, len(sys.argv)):
        # The directory with all the logfiles
        logfile_dir = sys.argv[i]
        logfile = os.path.join(logfile_dir, "logfileC")
        logfiles.append(logfile)
    base_logfile_dir = sys.argv[1]

    e2e_latencies = {}

    eval_adaptive = len(sys.argv) == 4

    for logfile in logfiles:
        start_text = 'start time'
        _, appl_values = read_logs(logfile, start_text)
        for appl in appl_values:
            e2e = [(exec_times[appl] + a) for a in appl_values[appl]]
            if appl not in e2e_latencies:
                e2e_latencies[appl] = [e2e]
            else:
                e2e_latencies[appl].append(e2e)

    if eval_adaptive:
        labels = ['vs Fixed Keep-Alive', 'vs Adaptive Keep-Alive']
    else:
        labels = ['Factor of Improvement (vs Fixed Keep-Alive)']
    violin_ratios = []
    ratio_bin_edges = []
    ratio_cum_hist = []
    appl_ratio_bin_edges = {}
    appl_ratio_cum_hist = {}
    for l in range(len(labels)):
        ratios = []
        for appl in e2e_latencies:
            length = min(len(e2e_latencies[appl][0]),
                         len(e2e_latencies[appl][l + 1]))
            appl_ratio = [
                none / dedup for dedup, none in zip(
                    sorted(e2e_latencies[appl][0][:length]),
                    sorted(e2e_latencies[appl][l + 1][:length]))
            ]
            ratios.extend(appl_ratio)

            appl_r_hist, appl_r_bin_edges = np.histogram(appl_ratio, bins=BINS)
            appl_r_cum_hist = np.cumsum(appl_r_hist).astype(float)
            appl_r_cum_hist /= appl_r_cum_hist[-1]
            if appl in appl_ratio_cum_hist:
                appl_ratio_bin_edges[appl].append(appl_r_bin_edges)
                appl_ratio_cum_hist[appl].append(appl_r_cum_hist)
            else:
                appl_ratio_bin_edges[appl] = [appl_r_bin_edges]
                appl_ratio_cum_hist[appl] = [appl_r_cum_hist]

        violin_ratios.append(ratios)

        r_hist, r_bin_edges = np.histogram(ratios, bins=BINS)
        r_cum_hist = np.cumsum(r_hist).astype(float)
        r_cum_hist /= r_cum_hist[-1]
        ratio_bin_edges.append(r_bin_edges)
        ratio_cum_hist.append(r_cum_hist)

    def forward(x):
        return 0.5 + np.tan(3.05 * (x - 0.5))

    def inverse(x):
        return 0.5 + np.arctan(x - 0.5) / 3.05

    # Plot the Application wise factor of improvement
    for i in range(len(labels)):
        for appl in appl_names:
            axs[i].plot(appl_ratio_bin_edges[appl][i][:-1],
                        appl_ratio_cum_hist[appl][i],
                        label=appl_names[appl],
                        linewidth=2)
        xticks = np.linspace(0, 3, 7)
        axs[i].set_xticks(xticks)
        axs[i].set_xticklabels(['%.1f' % x for x in xticks],
                               fontsize=18,
                               fontweight='bold')
        axs[i].set_xlim(0, 3)
        axs[i].set_xlabel('{0}'.format(labels[i]),
                          fontsize=18,
                          fontweight='bold')
        if i == len(labels) - 1:
            plt.legend(loc="lower center",
                       ncol=5,
                       prop={
                           'weight': 'bold',
                           'size': 14
                       },
                       frameon=False,
                       bbox_to_anchor=(-0.2, 1),
                       columnspacing=1)
        # else :
        #   axs[i].legend(loc="lower right", ncol=1, prop={'size': 18})

    i = 0
    for ax in axs:
        ax.set_yscale('function', functions=(forward, inverse))
        ax.grid(True, axis='y')
        ax.spines['left'].set_position(('data', 0))

        yticks = [0, 0.005, 0.01, 0.05, 0.5, 0.95, 0.99, 0.995, 1.0]
        ax.set_yticks(yticks)
        ax.set_ylim(0, 1.0)
        if i == 0:
            ax.set_yticklabels(['%.3f' % y for y in yticks],
                               fontsize=18,
                               fontweight='bold')
            ax.set_ylabel('CDF of requests', fontsize=18, fontweight='bold')
        else:
            ax.set_yticklabels([])
        i += 1


fig = plt.figure(figsize=(10, 5))
ax_cdf0 = fig.add_subplot(1, 2, 1)
ax_cdf1 = fig.add_subplot(1, 2, 2)
cdf_subplot([ax_cdf0, ax_cdf1])

INCLUDE_SUBFIG_LABEL = False
if INCLUDE_SUBFIG_LABEL:
    plt.title('(a)', {'weight': 'bold', 'size': 18}, y=-0.3, x=-0.1)
    plt.subplots_adjust(bottom=0.20,
                        top=0.84,
                        left=0.14,
                        right=0.97,
                        wspace=0.15)
else:
    plt.subplots_adjust(bottom=0.14,
                        top=0.84,
                        left=0.11,
                        right=0.97,
                        wspace=0.15)

base_logfile_dir = sys.argv[1]
plt.savefig(os.path.join(base_logfile_dir, 'cdfs.pdf'))
