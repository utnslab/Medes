"""Run analysis for the motivation experiments"""
import os
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import pickle as pkl
from calc_dedup import calc, calc_rabin

# Plot duplication numbers for all chunk sizes
apps = 10

usecase = [
    'Vanilla', 'LinAlg', 'ImagePro', 'VideoPro', 'MapReduce', 'HTTPServe',
    'AuthEnc', 'FeatureGen', 'ModelServe', 'ModelTrain'
]
mapping = {0: 0, 1: 4, 2: 5, 3: 6, 4: 2, 5: 3, 6: 1, 7: 7, 8: 9, 9: 8}

plt.rcParams['text.usetex'] = True #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [r'\usepackage{sansmath}', r'\sansmath'] #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif' # ... for regular text
plt.rcParams['font.sans-serif'] = 'Computer Modern Sans serif' # Choose a nice font here

# Plot cross duplication numbers
confusion = []
data_labels = []
for _ in range(apps):
    data_labels.append('')
    confusion.append([0] * apps)

for app1 in range(apps):
    for app2 in range(apps):
        if app1 < app2:
            dir1 = os.path.join('/tmp/checkpoints/', 'cont' + str(2 * app1))
            dir2 = os.path.join('/tmp/checkpoints/', 'cont' + str(2 * app2))
        elif app1 > app2:
            continue
        else:
            dir1 = os.path.join('/tmp/checkpoints/', 'cont' + str(2 * app1))
            dir2 = os.path.join('/tmp/checkpoints/',
                                'cont' + str(2 * app2 + 1))
        p1, p2 = calc_rabin(dir1, dir2, 64)
        confusion[app1][app2] = p1
        confusion[app2][app1] = p2

with open('heatmap.pkl', 'wb') as f:
    pkl.dump(confusion, f)

with open('heatmap.pkl', 'rb') as f:
    confusion_pkl = pkl.load(f)

for app1 in range(apps):
    for app2 in range(apps):
        confusion[mapping[app1]][mapping[app2]] = confusion_pkl[app1][app2]
        confusion[mapping[app2]][mapping[app1]] = confusion_pkl[app2][app1]

for a in range(apps):
    appl = usecase[a]
    data_labels[mapping[a]] = appl

# heatmap = np.array(confusion)
heatmap = pd.DataFrame(confusion, columns=data_labels, index=data_labels)
ax = sns.heatmap(heatmap,
                 annot=True,
                 annot_kws={"size": 12},
                 cbar_kws={
                     "orientation": "horizontal",
                     "pad": 0.04
                 },
                 cmap="rocket_r")
cbar = ax.collections[0].colorbar
cbar.ax.tick_params(labelsize=14, color='black')
# ax.figure.axes[-1].xaxis.set_xticklabels(ax.figure.axes[-1].xaxis.get_xticklabels, fontsize=14, color='black')
ax.xaxis.tick_top()  # x axis on top
ax.xaxis.set_label_position('top')
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, fontweight='bold')
ax.set_yticklabels(ax.get_yticklabels(), fontweight='bold')
plt.tick_params(axis='both',
                which='major',
                labelsize=12,
                labelbottom=False,
                bottom=False,
                top=False,
                labeltop=True,
                color='black')

plt.savefig('motiv2.pdf', bbox_inches='tight')
