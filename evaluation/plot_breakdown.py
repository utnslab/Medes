import argparse
import ast

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FormatStrFormatter

plt.rcParams['text.usetex'] = True #Let TeX do the typsetting
plt.rcParams['text.latex.preamble'] = [r'\usepackage{sansmath}', r'\sansmath'] #Force sans-serif math mode (for axes labels)
plt.rcParams['font.family'] = 'sans-serif' # ... for regular text
plt.rcParams['font.sans-serif'] = 'Computer Modern Sans serif' # Choose a nice font here

# ================== Configuration ==================
XLABEL='Function'
YLABEL='Time (ms)'
YTICKS=np.linspace(0, 2000, 5)
YTICKLABELS=['%.0f' % tick for tick in YTICKS ]

SCALE=1

LABELS = {
  'cs': 'Cold start',
  'read': 'Dedup: base page reading',
  'compute': 'Dedup: original page computing',
  'restore': 'Dedup: sandbox restoration'
}
COLORS = {
          'cs': 'gray',
          'read': '#1f77b4', 
          'compute': '#ff7f0e',
          'restore': '#2ca02c'}
          
HATCHES ={
          'cs': '',
          'read': '//', 
          'compute': '..',
          'restore': '++'}

BARWIDTH = 0.3
NBARS = 2

# ================== Data IO ==================
argparser = argparse.ArgumentParser('plot_breakdown')
argparser.add_argument('--input', type=str, default='/dev/stdin', help='Input file to read from (default: /dev/stdin)')
argparser.add_argument('--output', type=str, default='breakdown.pdf', help='Output file (default: $PWD/breakdown.pdf)')
args = argparser.parse_args()

fd = open(args.input, 'r')
cst_raw = None
dst_raw = None
while True:
  line = fd.readline()
  if 'cst' in line:
    cst_raw = line.split('=')[-1].strip()
  elif 'dst' in line:
    dst_raw = line.split('=')[-1].strip()
  
  if cst_raw is not None and dst_raw is not None:
    break
fd.close()

cst = np.array(ast.literal_eval(cst_raw))
dst = np.array(ast.literal_eval(dst_raw))
base_page_ts = dst[:,0]
og_page_comp_ts = dst[:,1]
restore_ts = dst[:,2]

appl_names = {
  0: "Vanilla",
  1: "LinAlg",
  2: "ImagePro",
  3: "VideoPro",
  4: "MapReduce",
  5: "HTMLServe",
  6: "AuthEnc",
  7: "FeatureGen",
  8: "RNNModel",
  9: "ModelTrain"
}

xs = [ appl_names[i] for i in range(10) ]

all_data = {}
all_data['cs'] = cst
all_data['read'] = base_page_ts
all_data['compute'] = og_page_comp_ts
all_data['restore'] = restore_ts


# ================== Plot ==================
fig = plt.figure(figsize=(10,4)) # 6.4:4.8
ax = fig.add_subplot(111)

i=NBARS/2
bottoms_base = { x:0 for x in xs }

for label, data in all_data.items():
  if not label.startswith('cs'):
        continue
  part =label
  for j, nflow, barh in zip(range(len(xs)), xs, data):
    ax.bar(j + (i-NBARS)*BARWIDTH, barh * SCALE, BARWIDTH, bottom=bottoms_base[nflow],
      align='edge', color=COLORS[part], edgecolor='black', hatch=HATCHES[part])
    bottoms_base[nflow] += barh * SCALE
i+=1

bottoms = { x:0 for x in xs }
for x in xs:
  bottoms[x] = 0
for label, data in all_data.items():
  if label.startswith('cs'):
    continue
  for j, nflow, barh in zip(range(len(xs)), xs, data):
    ax.bar(j + (i-NBARS)*BARWIDTH, barh * SCALE, BARWIDTH,bottom=bottoms[nflow],
      align='edge', color=COLORS[label], edgecolor='black', hatch=HATCHES[label])
    bottoms[nflow] += barh * SCALE
    # ax.text(j + (i+1-NBARS)*BARWIDTH+0.005, bottoms[nflow]-barh * SCALE/2, '%.1f' % barh * SCALE, color='black', fontsize=14)

patches = [mpatches.Patch(facecolor=COLORS[k], label=LABELS[k], edgecolor='black', hatch=HATCHES[k])
  for k in all_data.keys()]

ax.legend(handles=patches, loc = "lower center", ncol=2, prop={'weight':'bold', 'size':16}, bbox_to_anchor=(0.5, 0.95), frameon=False)

ax.set_xticks([i for i in range(len(xs))])
ax.set_xticklabels(xs, fontsize=18, fontweight='bold', rotation=22.5)
ax.set_yticks(YTICKS)
ax.set_yticklabels(YTICKLABELS, fontsize=18, fontweight='bold')
ax.set_yscale('log')
ax.yaxis.set_major_formatter(FormatStrFormatter("%.0f"))

ax.grid(True, axis='y')
ax.set_xlabel(XLABEL, fontsize=18, fontweight='bold')
ax.set_ylabel(YLABEL, fontsize=18, fontweight='bold')

plt.subplots_adjust(bottom=0.25,top = 0.8, left= 0.1, right=0.97)

# plt.show()
plt.savefig(args.output, format=args.output.split('.')[-1])