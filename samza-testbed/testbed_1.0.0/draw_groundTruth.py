import sys
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

root_dir = ""
output_file = None
epoch_length = 100

def write_file(executor, time, latency):
    # file_name = root_dir + output_file
    line = executor + ":" + str(time) + ":" + str(latency)+"\n"
    output_file.write(line)


def get_data(file_name):
    file = open(file_name)
    executor = 0
    time, ground_truth = [], []
    start, last_epoch = 0, 0
    tmp_set = []

    for line in file.readlines():
        if line[:4] == "pid=":
            executor = line[4:10]
        words = line.split(":")
        if words[0] != "stock_id":
            continue
        arrival_time = int(words[2][1:-14])
        completion_time = int(words[3][1:])
        if start == 0:
            start = completion_time
        latency = completion_time - arrival_time
        epoch = (completion_time - start) // epoch_length
        # if executor == "000013":
        #     print(epoch)
        if epoch == last_epoch:
            tmp_set.append(latency)
        elif len(tmp_set) > 0:
            avg_latency = sum(tmp_set)/len(tmp_set)
            tmp_set=[]
            time.append(last_epoch)
            ground_truth.append(avg_latency)
            last_epoch = epoch
            tmp_set.append(latency)
            write_file(executor, time[-1], latency)

    return executor, time, ground_truth


def calibrate(time, latency):
    if time[0] != 0:
        time.insert(0, 0)
        latency.insert(0,0)
    time_res = [time[0]]
    latency_res = [latency[0]]
    for i in range(len(time) - 1):
        t0 = time[i]
        t1 = time[i+1]
        a0 = latency[i]
        a1 = latency[i+1]
        if t1 - t0 >= 1 :
            for t in range(t0 + 1, t1 + 1):
                d = (t-t0) * (a1 - a0) / (t1 - t0) + a0
                time_res.append(t)
                latency_res.append(d)
    return time_res, latency_res


def write_max_latency(times, latencies):
    times_c, latencies_c = {}, {}
    max_time = 0

    max_latency_x, max_latency_y = [], []
    max_output_file = open(root_dir + "/maxLatency.txt", "w")

    for executor in times.keys():
        time = times[executor]
        latency = latencies[executor]
        time_2, latency_2 = calibrate(time, latency)
        times_c[executor] = time_2
        latencies_c[executor] = latency_2
        if max_time < time[-1]:
            max_time = time[-1]
    for i in range(0, max_time):
        max_latency = 0
        for latency_c in latencies_c.values():
            if len(latency_c) > i and latency_c[i] > max_latency:
                max_latency = latency_c[i]
        max_latency_x.append(i)
        max_latency_y.append(max_latency)
        line = str(i) + ":" + str(max_latency) + "\n"
        max_output_file.write(line)
    draw_plot({"max":max_latency_x}, {"max":max_latency_y}, "/maxLatency")
    max_output_file.close()
    #draw_plot(times_c, latencies_c)


def draw_plot(data_x, data_y, name):
    plt.figure(figsize=(20,12))
    plt.xticks(fontsize=40)
    plt.yticks(fontsize=40)
    plt.xlabel("Time/s", fontsize=40)
    plt.ylabel("Latency/ms", fontsize=40)
    plt.title("", fontsize=40)
    for executor, x in data_x.items():
        new_x = [i / 10 for i in x]
        plt.plot(new_x, data_y[executor], label = executor)
    plt.yscale('log')
    plt.ylim(1,1E6)
    plt.legend(fontsize=20)
    plt.savefig(root_dir+name)
    # plt.show()


if __name__ == "__main__":
    root_dir = sys.argv[1]
    # root_dir = "/home/drg/PycharmProjects/work2/results/giraffe/application_1617796365893_0032"
    output_file = open(root_dir + "/groundTruth.txt", "a+")
    data_x, data_y = {}, {}
    for root1, dirs1, files1 in os.walk(root_dir):
        for sub_dir in dirs1:
            if sub_dir[-9:-7] == "02":
                quit()
            if sub_dir[-6:] == "000001":
                continue
            for root2, dirs2, files2 in os.walk(root1+"/"+sub_dir):
                for file in files2:
                    if file == "stdout":
                        executor, time, ground_truth = get_data(root2+"/"+file)
                        data_x[executor] = time
                        data_y[executor] = ground_truth

    output_file.close()
    draw_plot(data_x, data_y, "/groundTruth")
    write_max_latency(data_x, data_y)

