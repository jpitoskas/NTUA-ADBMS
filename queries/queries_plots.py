import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import pandas as pd

def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{0:.2f}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 5),
                    fontsize=8,
                    textcoords="offset points",
                    ha='center', va='center')

times = defaultdict(list)

f = open("output/rdd/times.txt", "r")
for line in f.readlines():
    info = line.rstrip().split(":")
    label = info[0]
    time = float(info[1])
    times[label].append(time)
f.close()

f = open("output/sql/csv/times.txt", "r")
for line in f.readlines():
    info = line.rstrip().split(":")
    label = info[0]
    time = float(info[1])
    times[label].append(time)
f.close()

f = open("output/sql/parquet/times.txt", "r")
for line in f.readlines():
    info = line.rstrip().split(":")
    label = info[0]
    time = float(info[1])
    times[label].append(time)
f.close()

labels = list(times.keys())
times_per_query = list(times.values())
all_times = []
for i in range(len(times_per_query[0])):
    res = []
    for query in times_per_query:
        res.append(query[i])
    all_times.append(res)
    

df = pd.DataFrame({ 'RDD': all_times[0],'SQL with CSV': all_times[1], 'SQL with Parquet': all_times[2] }, 
                    index=labels)

# print(df)

ax = df.plot.bar(rot=0, width=0.8)

fig = ax.get_figure()

ax.set_title('Execution times grouped by query')
ax.set_ylabel('Time (sec)')
ax.grid(b = True, color ='grey', linestyle ='-.', linewidth = 0.5, alpha = 0.2)
# for s in ['top', 'bottom', 'left', 'right']:
for s in ['top', 'right']:
    ax.spines[s].set_visible(False)
ax.legend()

autolabel(ax.patches)

fig.tight_layout()
plt.show()

# fig.savefig("queries_plots.png")