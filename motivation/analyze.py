"""Run analysis for the motivation experiments"""
import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pickle as pkl

from seaborn import distributions
from calc_dedup import calc, calc_rabin

# sns.set_theme()

# Plot duplication numbers for all chunk sizes
# chunks = [64, 128, 256, 512, 1024, 2048, 4096]
chunks = [64, 128, 256, 512, 1024]
apps = 10

plt.rcParams['text.usetex'] = True #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [r'\usepackage{sansmath}', r'\sansmath'] #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif' # ... for regular text
plt.rcParams['font.sans-serif'] = 'Computer Modern Sans serif' # Choose a nice font here

usecase = [
    'Vanilla', 'LinAlg', 'ImagePro', 'VideoPro', 'MapReduce', 'HTTPServe',
    'AuthEnc', 'FeatureGen', 'ModelServe', 'ModelTrain'
]

vschunk = []
for app in range(apps):
    dir1 = os.path.join('/tmp/checkpoints/', 'cont' + str(2 * app))
    dir2 = os.path.join('/tmp/checkpoints/', 'cont' + str(2 * app + 1))
    dupl = []
    for chunk in chunks:
        # p1, p2 = calc(dir1, dir2, chunk)
        p1, p2 = calc_rabin(dir1, dir2, chunk)
        dupl.append(max(p1, p2))
    with open('{0}.pkl'.format(app), 'wb') as f:
        pkl.dump(dupl, f)
    vschunk.append(dupl)

vschunk = []
for app in range(apps):
    with open(os.path.join(sys.argv[1], '{0}.pkl'.format(app)), 'rb') as f:
        dupl = pkl.load(f)
    vschunk.append(dupl)    

fig = plt.figure(figsize=(5.6, 4.8))  # 6.4:4.8
ax = fig.add_subplot(111)

for i in range(apps):
    dupl = vschunk[i]
    ax.plot(chunks, dupl, label=usecase[i], linewidth=3)
ax.legend(loc="center left", ncol=1, prop={'weight': 'bold', 'size': 14}, bbox_to_anchor=(1.04, 0.5))
ax.set_xscale('log')

yticks = np.linspace(0, 1, 11)
xticks = np.logspace(6, 10, 5, base=2)
print(xticks)

ax.set_yticks(yticks)
ax.set_yticklabels(['%.1f' % x for x in yticks],
                   fontsize=18,
                   fontweight='bold')
ax.set_xticks(xticks)
ax.set_xticklabels([str(int(x)) for x in xticks],
                   fontsize=18,
                   fontweight='bold')

ax.set_ylim(0.5, 1.0)

# ax.set_aspect('equal')
ax.grid(True, axis='y')
ax.set_xlabel('Chunk size (B)', fontsize=18, fontweight='bold')
ax.set_ylabel('Memory redundancy', fontsize=18, fontweight='bold')

plt.subplots_adjust(bottom=0.13, top=0.96, left=0.14, right=0.66)

plt.savefig('motiv1.pdf')
plt.close()
