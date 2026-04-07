import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RESULTS_DIR = '/workspace/results'
METRICS_FILE = os.path.join(RESULTS_DIR, 'metrics.csv')

if not os.path.exists(METRICS_FILE):
    raise SystemExit('metrics.csv not found')

df = pd.read_csv(METRICS_FILE)
df = df.sort_values('experiment').reset_index(drop=True)

x = np.arange(len(df))
width = 0.25

plt.figure(figsize=(12, 6))
plt.bar(x - width, df['total_time_sec'], width, label='Total time')
plt.bar(x, df['kmeans_time_sec'], width, label='KMeans time')
plt.bar(x + width, df['groupby_time_sec'], width, label='GroupBy time')

plt.xticks(x, df['experiment'], rotation=20)
plt.ylabel('Seconds')
plt.title('Spark experiments timing comparison')
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(RESULTS_DIR, 'all_times.png'))
plt.close()
