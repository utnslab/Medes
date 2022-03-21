"""Read log files and pickle files to plot macro benchmarks

Usage:
python plot_memory.py <Log directory 1> <Log directory 2> <Number of agents>
"""
import os
import sys
import math
import pickle
import matplotlib.pyplot as plt
from datetime import datetime
from itertools import repeat
import numpy as np

plt.rcParams['text.usetex'] = True  #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [
    r'\usepackage{sansmath}', r'\sansmath'
]  #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif'  # ... for regular text
plt.rcParams[
    'font.sans-serif'] = 'Computer Modern Sans serif'  # Choose a nice font here


def get_first_timestamp(log_file, search_text):
    """Get the first timestamp of `search_text' in the log_file

    Args:
        log_file
        search_text (str)

    Returns:
        timestamp: datetime object
    """
    timestamp = None
    with open(log_file, "r") as f:
        content = [next(f) for x in range(51)]
    content = [x.strip().split() for x in content]

    for log_line in content:
        # Extract text
        text = ' '.join(log_line[3:-1])
        if search_text in text.lower():
            # Extract date for this text
            date_time_str = ' '.join(log_line[0:2])[1:-1]
            timestamp = datetime.strptime(date_time_str,
                                          '%Y-%m-%d %H:%M:%S.%f')
            break

    return timestamp


def get_spurious_usage(log_file):
    """Get spurious container memory usage"""
    # List of tuples (start time, subtract memory)
    memory = []
    with open(log_file, "r", encoding='latin-1') as f:
        content = f.readlines()
    content = [x.strip().split() for x in content]
    start_time = None

    prev_value = 0
    for log_line in content:
        # Extract text
        text = ' '.join(log_line[3:-1])

        if start_time == None:
            try:
                date_time_str = ' '.join(log_line[0:2])[1:-1]
                start_time = datetime.strptime(date_time_str,
                                               '%Y-%m-%d %H:%M:%S.%f')
            except:
                continue

        if 'containers: w' in text.lower():
            try:
                date_time_str = ' '.join(log_line[0:2])[1:-1]
                date_time_obj = datetime.strptime(date_time_str,
                                                  '%Y-%m-%d %H:%M:%S.%f')
            except:
                continue

            valid_containers = (int(log_line[-12]) + int(log_line[-10]) +
                                int(log_line[-8]) + int(log_line[-6]))
            total_containers = int(log_line[-1])
            if not ((total_containers - valid_containers) == prev_value):
                prev_value = total_containers - valid_containers
                seconds_elapsed = (date_time_obj - start_time).total_seconds()
                memory.append((int(seconds_elapsed), prev_value * 45))

    return memory


def subtract_spurious_usage(tmu, spurious_mem):
    prev_tup = None
    max_rem = 0
    for tup in spurious_mem:
        curr_time = tup[0]
        if prev_tup != None and prev_tup[0] < len(tmu):
            for i in range(prev_tup[0], min(len(tmu), curr_time)):
                tmu[i] -= prev_tup[1]
        prev_tup = tup
        max_rem = max(max_rem, prev_tup[1])
    return tmu


def get_memusage_from_dict(stats_dir, agents):
    """Read memory usage dictionary from the stats_dir of all agents and collapse them

    Args:
        stats_dir
        agents (int): number of agents

    Returns:
        Four lists of reported memory usage, total memory usage, DedupAgent usage and checkpoints size
    """
    # Plot memory stats
    all_agents_totalmem = []
    all_agents_mem = []
    all_agents_appl = []
    all_agents_checkpoints = []
    max_length = 1

    for agent in range(agents):
        exp_start = get_first_timestamp(
            os.path.join(stats_dir, "logfile" + str(agent)), "memory layer")
        cold_start = get_first_timestamp(
            os.path.join(stats_dir, "logfile" + str(agent)), "cold start time")
        if cold_start == None:
            cold_start = exp_start
        start_offset = math.floor((cold_start - exp_start).total_seconds())
        time_offset = math.floor((cold_start - exp_start).total_seconds()) + 60

        try:
            with open(stats_dir + "/memory_stats_agent" + str(agent) + ".pkl",
                      "rb") as f:
                mem_dict = pickle.load(f)
                mem_usage = mem_dict['used']
                checkpoint_usage = mem_dict['checkpoints']
                if 'appl' in mem_dict:
                    appl_usage = mem_dict['appl']
                else:
                    appl_usage = [0] * len(mem_dict['checkpoints'])

                try:
                    # Print the significant stats from breakdown
                    breakdown = mem_dict['breakdown']
                    found = False
                    for index in range(len(breakdown)):
                        if len(breakdown[index]) > 0:
                            found = True
                    if not found:
                        print("No significant process found")
                except:
                    pass

                initial_usage = mem_usage[start_offset]
                # initial_usage = 0
                mem_usage = [x - initial_usage for x in mem_usage]
                total_usage = [
                    x + y for x, y in zip(mem_usage, checkpoint_usage)
                ]

                all_agents_mem.append(mem_usage[time_offset:])
                all_agents_totalmem.append(total_usage[time_offset:])
                all_agents_appl.append(appl_usage[time_offset:])
                all_agents_checkpoints.append(checkpoint_usage[time_offset:])
                max_length = min(max(max_length, len(mem_usage[time_offset:])),
                                 3600)
        except:
            all_agents_mem.append([1000] * max_length)
            all_agents_totalmem.append([1000] * max_length)
            all_agents_appl.append([1000] * max_length)
            all_agents_checkpoints.append([1000] * max_length)
            continue

    # Keep the lengths of all agents same (for valid comparisons)
    for agent in range(agents):
        diff = max_length - len(all_agents_mem[agent])
        if diff > 0:
            all_agents_mem[agent].extend(
                repeat(all_agents_mem[agent][-1], diff))
            all_agents_totalmem[agent].extend(
                repeat(all_agents_totalmem[agent][-1], diff))
            all_agents_appl[agent].extend(
                repeat(all_agents_appl[agent][-1], diff))
            all_agents_checkpoints[agent].extend(
                repeat(all_agents_checkpoints[agent][-1], diff))
        else:
            del all_agents_mem[agent][max_length:]
            del all_agents_totalmem[agent][max_length:]
            del all_agents_appl[agent][max_length:]
            del all_agents_checkpoints[agent][max_length:]

    mem_usage_all = [sum(x) for x in zip(*all_agents_mem)]
    totalmem_usage_all = [sum(x) for x in zip(*all_agents_totalmem)]
    appl_usage_all = [sum(x) for x in zip(*all_agents_appl)]
    checkpoint_usage_all = [sum(x) for x in zip(*all_agents_checkpoints)]

    return mem_usage_all, totalmem_usage_all, appl_usage_all, checkpoint_usage_all


num_dirs = len(sys.argv) - 2
agents = int(sys.argv[-1])
stats_dir = []
mu = []
tmu = []
for i in range(num_dirs):
    stats_dir.append(sys.argv[i + 1])
    _mu, _tmu, _, _ = get_memusage_from_dict(stats_dir[-1], agents)
    mu.append(_mu)

    if i == 0:
        for a in range(agents):
            m = get_spurious_usage(
                os.path.join(stats_dir[-1], 'logfile' + str(a)))
            _tmu = subtract_spurious_usage(_tmu, m)
    tmu.append(_tmu)

# print('Total benefit: ', (sum(tmu2) - sum(tmu1)) / sum(tmu2))
# print('Max headroom: ', (max(tmu2) - max(tmu1)) / max(tmu2))
# print('Max utilization: ', max(tmu1), max(tmu2))

labels = ['Medes', 'Fixed\nKeep-Alive', 'Adaptive\nKeep-Alive']
tmus = []
for i in range(len(labels)):
    print(sum(tmu[i]) / (len(tmu[i]) * 38000))
    plt.plot(tmu[i], label=labels[i])
    tmus.append([x / 1000 for x in tmu[i]])

plt.legend()
plt.savefig(os.path.join(stats_dir[0], "memory_comp.png"))
plt.close()

fig = plt.figure(figsize=(4.8, 6.4))  # 6.4:4.8
ax = fig.add_subplot(111)

# Plot box plots
ax.boxplot(tmus,
           whis=(10, 90),
           showfliers=False,
           showmeans=True,
           meanline=True,
           labels=labels,
           widths=0.25,
           boxprops={'linewidth': 3},
           medianprops={
               'linewidth': 3,
               'linestyle': ':'
           },
           meanprops={
               'linewidth': 3,
               'linestyle': '-'
           },
           capprops={'linewidth': 5},
           whiskerprops={'linewidth': 3})

ax.set_ylabel("Cluster memory usage (GB)", fontsize=16, fontweight='bold')
# ax.set_xlabel("Policy", fontsize=18, fontweight='bold')

yticks = np.linspace(0, 40, 5)
ax.set_yticks(yticks)
ax.set_yticklabels([str(int(x)) for x in yticks],
                   fontsize=14,
                   fontweight='bold')
ax.set_ylim(0, 40)
ax.set_xticklabels(labels, fontsize=16, fontweight='bold')

patches = [
    ax.plot([], [], color="#2ca02c", label="Mean", linewidth=5),
    ax.plot([], [], color="#ff7f0e", label="Median", linewidth=5)
]
# ax.legend(loc="upper right", ncol=1, prop={'weight': 'bold', 'size': 14})
ax.legend(loc="lower center",
          ncol=2,
          prop={
              'weight': 'bold',
              'size': 16
          },
          bbox_to_anchor=(0.5, 0.98),
          columnspacing=1,
          frameon=False)

plt.subplots_adjust(bottom=0.1, top=0.95, left=0.15, right=0.96)
plt.savefig(os.path.join(stats_dir[0], "memory_box.pdf"))
plt.close()