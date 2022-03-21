"""
Read log file and plot the number of warm and running containers
[For any Dedup None policy]

Arguments:
1: Directory of logfiles
2: Number of agents

Usage:
python plot_warm.py ./motivation/808 4
"""
import os
import sys
import datetime
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams['text.usetex'] = True #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [r'\usepackage{sansmath}', r'\sansmath'] #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif' # ... for regular text
plt.rcParams['font.sans-serif'] = 'Computer Modern Sans serif' # Choose a nice font here

def count_containers(log_file):
    """Reads the agent logs and calculates the number of warm containers in each state at every time

    Args:
        log_file (str) : Path to the agent log file

    Returns:
        A list of number of warm containers
    """
    num_warm = []
    num_cont = []

    with open(log_file, "r") as f:
        content = f.readlines()
    content = [x.strip().split() for x in content]

    for log_line in content:
        # Extract text
        text = ' '.join(log_line[3:])
        search_text = "num warm containers"
        if search_text in text.lower():
            warm = int(log_line[-2])
            total = int(log_line[-1])
            num_warm.append(warm)
            num_cont.append(total)

    return num_warm, num_cont


# The directory with the logfiles
logfile_dir = sys.argv[1]
agents = int(sys.argv[2])
warm_conts = []
total_conts = []

dedup_info = {
    'Vanilla': [0.85, 15],
    'LinAlg': [0.85, 35],
    'ImagePro': [0.85, 26],
    'VideoPro': [0.85, 35],
    'MapReduce': [0.85, 25],
    'HTTPServe': [0.85, 22],
    'AuthEnc': [0.85, 25],
    'FeatureGen': [0.85, 66],
    'ModelServe': [0.85, 60],
    'ModelTrain': [0.85, 87.5]
}

for agent in range(agents):
    log_file = os.path.join(logfile_dir, "logfile" + str(agent))
    warm, total = count_containers(log_file)
    warm_conts.append(warm)
    total_conts.append(total)

num_warm = [sum(x) for x in zip(*warm_conts)]
num_total = [sum(x) for x in zip(*total_conts)]
running_cont = [(x - y) for x, y in zip(num_total, num_warm)]

# Trial
# mem = [55 * (l[0] + l[2]) + 48 * l[2] + 44 * l[1] for l in num_cont]  # Appl 3
# mem = [40 * (l[0] + l[2]) + 35 * l[2] + 33 * l[1] for l in num_cont]  # Appl 1
# plt.plot(mem)
# plt.savefig(os.path.join(logfile_dir, "expected_mem.png"))
# plt.close()

# Make a stack plot for better comparison of total containers
labels = ['Running containers', 'Warm containers']
# plt.plot(warm_, label='Warm containers')
# plt.plot(dedup_, label='Dedup containers')
# plt.plot(base_, label='Base containers')

# plt.stackplot(list(range(len(num_warm))),
#               running_cont,
#               num_warm,
#               labels=labels)
# plt.ylabel('Number of containers')
# plt.xlabel('Elapsed time (s)')
# plt.legend(loc='upper left')
# plt.savefig(os.path.join(logfile_dir, "num_containers.png"))
# plt.close()

# Plot the maximum duplication possible across all containers
total_usage = []
dedup_usage_min = []
dedup_usage_max = []
dedup_usage_mean = []
max_memory = (1.5 * max(num_total))
print(max_memory)
for t in range(len(num_warm)):
    total_usage.append(float(num_total[t]) / max_memory)
    workload_usage = []
    for _, val in dedup_info.items():
        workload_usage.append(
            float(num_warm[t] *
                  (1 - val[0]) * val[1] + running_cont[t] * val[1]) /
            (max_memory * val[1]))
    dedup_usage_min.append(min(workload_usage))
    dedup_usage_max.append(max(workload_usage))
    dedup_usage_mean.append(sum(workload_usage) / len(workload_usage))

fig = plt.figure(figsize=(9,4)) # 6.4:4.8
ax = fig.add_subplot(111)

ax.plot(dedup_usage_mean, label='Usage after redudancy elimination', linewidth=3)
ax.plot(total_usage, label='Keep-Alive Memory Usage', linewidth=3)
# ax.fill_between(range(len(total_usage)),
#                  dedup_usage_min,
#                  dedup_usage_max,
#                  alpha=0.2)
ax.legend(loc='upper center', ncol=1,prop={'weight':'bold', 'size':18})

xticks=np.linspace(0, 2000, 5)
yticks=np.linspace(0, 1, 6)

ax.set_yticks(yticks)
ax.set_yticklabels([int(x*100) for x in yticks], fontsize=18, fontweight='bold')
ax.set_xticks(xticks)
ax.set_xticklabels([str(int(x)) for x in xticks], fontsize=18, fontweight='bold')

# ax.set_aspect('equal')
ax.grid(True, axis='y')
ax.set_xlabel('Elapsed time (s)', fontsize=18, fontweight='bold')
ax.set_ylabel('Memory usage (\%)', fontsize=18, fontweight='bold')

plt.subplots_adjust(bottom=0.16,top = 0.96, left= 0.16, right=0.96)
# ax.xticks([])
plt.savefig(os.path.join(logfile_dir, "max_benefit.pdf"))
plt.close()