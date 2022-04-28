import os
import sys
import util.Tools as tools
import matplotlib.pyplot as plt
from pylab import setp

benchmarks= {"Our Scheduling": "/default" , "Elasticutor":"/elasticutor"}

def read_max_latency(file_name):
    time, latency = [], []
    file = open(file_name)
    for line in file.readlines():
        time.append(int(line.split(":")[0]))
        latency.append(int(line.split(":")[1][:-1]))
    return time, latency


def sample_data(time, latency, start, end, granularity):
    time_sampling, latency_sampling = [], []
    for i in range(len(time)):
        if start < time[i] < end and time[i] % granularity == 0:
            time_sampling.append(time[i])
            latency_sampling.append(latency[i])

    # time_sampling = [i / 10.0 for i in time_sampling]
    # time_sampling = [i / 10.0 + 400 for i in time_sampling]
    time_sampling = [i / 10.0 + 200 for i in time_sampling]
    return time_sampling, latency_sampling


def draw_ground_truth(root):
    labels = {"x": "Time (s)", "y": "Latency (ms)", "saveFile": root}
    max_latencies = {}
    times = {}
    for label, dirct in benchmarks.items():
        file_name = root + dirct + "/maxLatency.txt"
        time, latency = read_max_latency(file_name)
        time_sampling, latency_sampling = sample_data(time, latency, 5000, 10000, 100)
        times[label] = time_sampling
        max_latencies[label] = latency_sampling
    draw_science(times, max_latencies, labels, benchmarks.keys())


def draw_science(x, y, labels, items):
    plt.figure(figsize=(7,4))

    left, width = 0.17, 0.74
    bottom, height = 0.20, 0.71
    rect_line = [left, bottom, width, height]
    plt.axes(rect_line)

    plt.xticks(fontsize=25)
    plt.yticks(fontsize=25)
    plt.xlabel(labels["x"], fontsize=25)
    plt.ylabel(labels["y"], fontsize=25)
    colors= {"Our Scheduling": 'darkorange' , "Elasticutor": 'darkblue' , "Elasticutor+Our Memory Scheduling": 'green'}
    linestyles= {"Our Scheduling": '-' , "Elasticutor": '--' , "Elasticutor+Our Memory Scheduling": '-.'}
    for item in items:
        plt.plot(x[item], y[item], label = item, linewidth=4, color = colors[item], linestyle = linestyles[item])
    plt.yscale('log')
    plt.grid()
    plt.ylim(1E1,1E7)
    plt.legend(fontsize=17,loc="upper center",ncol=2)
    plt.savefig(labels['saveFile']+'/MaxLatency.pdf')


if __name__ == "__main__":
    root = sys.argv[1]
    draw_ground_truth(root)