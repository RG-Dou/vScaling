import matplotlib
matplotlib.use('Agg')

import sys
import os
import Tools as tools
import matplotlib.pyplot as plt
from pylab import setp

def get_app_path(path):
    for root1, dirs1, files1 in os.walk(path):
        for sub_dir in dirs1:
            return sub_dir


def get_am_path(path):
    for root1, dirs1, files1 in os.walk(path):
        for sub_dir in dirs1:
            if sub_dir[-6:] == "000001":
                return sub_dir


def draw_max_latency(root, sub_dirs):
    xs, ys = {}, {}
    for key, dir in sub_dirs.items():
        dir_path = root + '/' +dir
        file = dir_path + '/' + get_app_path(dir_path) + '/maxLatency.txt'
        times, latencies = tools.read_max_latency(file)
        xs[key] = [i/10.0 for i in times]
        ys[key] = tools.get_avg(latencies, 100)
    plt_max_latency(xs, ys, sub_dirs.keys(), root)


def plt_max_latency(x, y, items, root):
    plt.figure(figsize=(14, 7))

    left, width = 0.10, 0.85
    bottom, height = 0.15, 0.80
    rect_line = [left, bottom, width, height]
    plt.axes(rect_line)

    plt.xticks(fontsize=25)
    plt.yticks(fontsize=25)
    plt.xlabel("Time (s)", fontsize=25)
    plt.ylabel("Latency (ms)", fontsize=25)
    linewidths = {"Both Scheduling": 4, "Both with Current Arrival Rate": 4,
                  "CPU Scheduling": 4, "Memory Scheduling": 4, "Static": 2}
    linestyles = {"Both Scheduling": '-', "Both with Current Arrival Rate": '--',
                  "CPU Scheduling": '-.', "Memory Scheduling": ':', "Static": '-'}
    colors = {"Both Scheduling": 'black', "Both with Current Arrival Rate": 'brown',
              "CPU Scheduling": 'darkgoldenrod', "Memory Scheduling": 'darkgreen', "Static": 'gray'}
    for item in items:
        plt.plot(x[item], y[item], label=item, linewidth=linewidths[item], linestyle=linestyles[item],
                 color=colors[item])
    plt.yscale('log')
    plt.ylim(1E0, 1E8)
    plt.xlim(left=200, right=2000)
    plt.grid(linestyle="--", alpha=0.8)
    plt.legend(fontsize=20, loc="upper center", ncol=3)
    plt.savefig(root + '/MaxLatency.pdf')
##    plt.show()



def fetch_am_messages(root, sub_dirs, metric, num_containers, start, length):
    datas = {}
    for key, dir in sub_dirs.items():
        datas[key] = []
        dir_path = root + '/' + dir
        file = dir_path + '/' + get_app_path(dir_path) + '/' + get_am_path(dir_path) + "/stdout"
        for i in range(num_containers):
            i = i + 2
            instance = "0000" + str(i)
            if i < 10:
                instance = "0" + instance

            time, data = tools.read_file(metric, file, instance)

            if metric == "executor pg major fault":
                avg_pg = []
                for i in range(0, len(time), 10):
                    if time[i] < start or time[i] > start + length:
                        continue
                    avg_pg.append((data[i] - data[i-100])/(time[i] - time[i-100]))
                datas[key].append(avg_pg)
            else:
                index1, index2 = 0, 0
                for i in range(len(time)):
                    if time[i] < start:
                        index1 = i
                    if time[i] < start + length:
                        index2 = i
                datas[key].append(data[index1:index2])
    return datas


def draw_box_plot(datas, labels, sub_dirs, num_containers, log, ylim):
    plt.figure(figsize=(15,6))
    left, width = 0.10, 0.85
    bottom, height = 0.15, 0.80
    rect_line = [left, bottom, width, height]
    plt.axes(rect_line)
    plt.xticks(fontsize=25)
    plt.yticks(fontsize=25)
    plt.xlabel(labels["x"],fontsize=30)
    plt.ylabel(labels["y"],fontsize=30)
    plt.title("", fontsize=40)
    if log:
        plt.yscale('log')
    plt.ylim(bottom=ylim['bottom'], top=ylim['top'])
    plt.grid(linestyle="--", alpha=0.8, axis='y')
    position1, position2 = [], []
    flierprops = dict(marker='o', markerfacecolor='r', markersize=1,
                      linestyle='none', markeredgecolor='gray')
    for i in range(num_containers):
        position1.append(i*3 + 1)
    for i in range(num_containers):
        data_executor = [datas[sub_dirs[0]][i], datas[sub_dirs[1]][i]]
        bp = plt.boxplot(data_executor, positions=[i*3 + 0.5, i*3 + 1.5], widths=0.6, notch=False, patch_artist = False, showfliers = True, flierprops=flierprops)
        setBoxColors(bp)

    plt.xticks(position1, [i+1 for i in range(num_containers)])

    hB, = plt.plot([1, 1], 'b-')
    hR, = plt.plot([1, 1], 'r-')
    plt.legend((hB, hR), (sub_dirs[0], sub_dirs[1]), fontsize=25)
    hB.set_visible(False)
    hR.set_visible(False)
    plt.savefig(labels['saveFile'])


def setBoxColors(bp):
    setp(bp['boxes'][0], color='blue')
    setp(bp['caps'][0], color='blue')
    setp(bp['caps'][1], color='blue')
    setp(bp['whiskers'][0], color='blue')
    setp(bp['whiskers'][1], color='blue')
    setp(bp['medians'][0], color='blue')
    setp(bp['medians'][0], color='blue', linewidth = 4)

    setp(bp['boxes'][1], color='red')
    setp(bp['caps'][2], color='red')
    setp(bp['caps'][3], color='red')
    setp(bp['whiskers'][2], color='red')
    setp(bp['whiskers'][3], color='red')
    setp(bp['medians'][1], color='red', linewidth = 4)


def plt_memory_allocation(root):
    sub_dirs = {"CPU Scheduling": 'CPU', "Both Scheduling": 'both'}
    datas = fetch_am_messages(root, sub_dirs, "configure memory", 4, 1000, 1000)
    labels = {"x": "Executor Index", "y": 'Configured Memory (MB)', "saveFile": root+"/ConfigMem.pdf"}
    ylim = {'bottom': 950, 'top': 1350}
    draw_box_plot(datas, labels, list(sub_dirs.keys()), 4, False, ylim)


def plt_page_fault(root):
    sub_dirs = {"CPU Scheduling": 'CPU', "Both Scheduling": 'both'}
    datas = fetch_am_messages(root, sub_dirs, "executor pg major fault", 4, 1000, 1000)
    labels = {"x": "Executor Index", "y": 'Major Page Fault / s', "saveFile": root+"/PageFault.pdf"}
    ylim = {'bottom': 1E0, 'top': 1E5}
    draw_box_plot(datas, labels, list(sub_dirs.keys()), 4, True, ylim)


def plt_latency(root):
    sub_dirs = {"CPU Scheduling": 'CPU', "Both Scheduling": 'both'}
    datas = fetch_am_messages(root, sub_dirs, "Instantaneous Delay", 4, 1000, 1000)
    labels = {"x": "Executor Index", "y": 'Latency (ms)', "saveFile": root+"/Latency.pdf"}
    ylim = {'bottom': 1E0, 'top': 1E7}
    draw_box_plot(datas, labels, list(sub_dirs.keys()), 4, True, ylim)


def draw_memory_info(root):
    plt_memory_allocation(root)
    plt_page_fault(root)
    plt_latency(root)


if __name__ == "__main__":
    root = sys.argv[1]
    sub_dirs = {'Both Scheduling': 'both', 'CPU Scheduling': 'CPU', 'Memory Scheduling': 'memory', 'Both with Current Arrival Rate': 'current', 'Static': 'static'}
    draw_max_latency(root, sub_dirs)
    draw_memory_info(root)
