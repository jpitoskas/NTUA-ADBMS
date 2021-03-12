import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{0:.2f}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 2),  # 2 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

times = defaultdict(list)

f = open("output/optimizer_times.txt", "r")
for line in f.readlines():
    info = line.rstrip().split(":")
    if info[0] == 'Y':
        label = "Disabled"
    else:
        label = "Enabled"
    time = float(info[1])
    times[label].append(time)
f.close()


labels = list(times.keys())
xtimes = [item for sublist in times.values() for item in sublist]

x = np.arange(len(labels))

fig, ax = plt.subplots()
bars = ax.bar(x, xtimes, width=0.35, label='Execution Times')

ax.set_title('Execution times with query optimizer enabled and disabled')
ax.set_ylabel('Time (sec)')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.grid(b = True, color ='grey', linestyle ='-.', linewidth = 0.5, alpha = 0.2)
# for s in ['top', 'bottom', 'left', 'right']:
for s in ['top', 'right']:
    ax.spines[s].set_visible(False)

autolabel(bars)

fig.tight_layout()
plt.show()