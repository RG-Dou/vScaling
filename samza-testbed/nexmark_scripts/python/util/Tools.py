import re

divider=10
words_size_1 = 3
words_size_2 = 2

def get_index(data, target):
    for i in range(len(data)):
        if data[i] < target:
            continue
        else:
            return i
    return len(data)-1


def exact_data(data, patten):
    result = []
    for tuple in data:
        value = tuple[patten]
        result.append(value)
    return result


def get_slope(data, time, range):
    time_start = range[0]
    time_end = range[1]
    index_start = get_index(time, time_start)
    index_end = get_index(time, time_end)
    slope = (data[index_end] - data[index_start])/(time[index_end] - time[index_start])
    return slope


def get_avg(data, length):
    result = []
    for i in range(len(data)):
        if i < length:
            result.append(data[i])
            # continue
        else:
            total = 0
            for j in range(i - length, i):
                total += data[j]
            result.append(total/length)
    return result


def get_rate(data, time):
    result = []
    for i in range(len(time)-1):
        result.append((data[i+1] - data[i]) / (time[i+1] - time[i]))
    result.append(result[-1])
    return result


def extract_config_mem(words, containerId):
    value = None
    for i in range(len(words)):
        result = [0]
        if containerId == "000002":
            result = re.findall(".*000002=<memory:(.*).*", words[i])
        elif containerId == "000003":
            result = re.findall(".*000003=<memory:(.*).*", words[i])
        elif containerId == "000004":
            result = re.findall(".*000004=<memory:(.*).*", words[i])
        elif containerId == "000005":
            result = re.findall(".*000005=<memory:(.*).*", words[i])
        elif containerId == "000006":
            result = re.findall(".*000006=<memory:(.*).*", words[i])
        elif containerId == "000007":
            result = re.findall(".*000007=<memory:(.*).*", words[i])
        elif containerId == "000008":
            result = re.findall(".*000008=<memory:(.*).*", words[i])
        elif containerId == "000009":
            result = re.findall(".*000009=<memory:(.*).*", words[i])
        elif containerId == "000010":
            result = re.findall(".*000010=<memory:(.*).*", words[i])
        elif containerId == "000011":
            result = re.findall(".*000011=<memory:(.*).*", words[i])
        elif containerId == "000012":
            result = re.findall(".*000012=<memory:(.*).*", words[i])
        elif containerId == "000013":
            result = re.findall(".*000013=<memory:(.*).*", words[i])
        if len(result) > 0:
            value = result[0]
    if value is None:
        return None
    if "}" in value:
        value = value[0:-1]
    return int(value)
    # config_mem = re.findall(".*000002=<memory:(.*)", words[2])[0]
    # return int(config_mem)


def extract_config_cpu(words, containerId):
    value = None
    for i in range(len(words)):
        result = []
        if containerId == "000002" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000003" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000004" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000005" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000006" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000007" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000008" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000009" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000010" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000011" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000012" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        elif containerId == "000013" and containerId in words[i]:
            result = re.findall(".*vCores:(.*)>.*", words[i+1])
        if len(result) > 0:
            value = result[0]
    if value is None:
        return None
    if "}" in value:
        value = value[0:-1]
    return int(value)
    # config_mem = re.findall(".*vCores:(.*)>.*", words[3])[0]
    # return int(config_mem)


def extract_float(words, containerId):
    value = None
    for i in range(len(words)):
        result = []
        if containerId == "000002":
            result = re.findall(".*000002=(.*).*", words[i])
        elif containerId == "000003":
            result = re.findall(".*000003=(.*).*", words[i])
        elif containerId == "000004":
            result = re.findall(".*000004=(.*).*", words[i])
        elif containerId == "000005":
            result = re.findall(".*000005=(.*).*", words[i])
        elif containerId == "000006":
            result = re.findall(".*000006=(.*).*", words[i])
        elif containerId == "000007":
            result = re.findall(".*000007=(.*).*", words[i])
        elif containerId == "000008":
            result = re.findall(".*000008=(.*).*", words[i])
        elif containerId == "000009":
            result = re.findall(".*000009=(.*).*", words[i])
        elif containerId == "000010":
            result = re.findall(".*000010=(.*).*", words[i])
        elif containerId == "000011":
            result = re.findall(".*000011=(.*).*", words[i])
        elif containerId == "000012":
            result = re.findall(".*000012=(.*).*", words[i])
        elif containerId == "000013":
            result = re.findall(".*000013=(.*).*", words[i])
        if len(result) > 0:
            value = result[0]
    if value is None:
        return None
    if "}" in value:
        value = value[0:-1]

    return float(value)


def extract_int(words, containerId):
    value = None
    for i in range(len(words)):
        result = []
        if containerId == "000002":
            result = re.findall(".*000002=(.*).*", words[i])
        elif containerId == "000003":
            result = re.findall(".*000003=(.*).*", words[i])
        elif containerId == "000004":
            result = re.findall(".*000004=(.*).*", words[i])
        elif containerId == "000005":
            result = re.findall(".*000005=(.*).*", words[i])
        elif containerId == "000006":
            result = re.findall(".*000006=(.*).*", words[i])
        elif containerId == "000007":
            result = re.findall(".*000007=(.*).*", words[i])
        elif containerId == "000008":
            result = re.findall(".*000008=(.*).*", words[i])
        elif containerId == "000009":
            result = re.findall(".*000009=(.*).*", words[i])
        elif containerId == "000010":
            result = re.findall(".*000010=(.*).*", words[i])
        elif containerId == "000011":
            result = re.findall(".*000011=(.*).*", words[i])
        elif containerId == "000012":
            result = re.findall(".*000012=(.*).*", words[i])
        elif containerId == "000013":
            result = re.findall(".*000013=(.*).*", words[i])
        if len(result) > 0:
            value = result[0]
    if value is None:
        return None
    if "}" in value:
        value = value[0:-1]
    return int(value)


def read_file(type, file_name, containerId = "000002"):
    file = open(file_name)
    x_list, y_list = [], []
    for line in file.readlines():
        x, y = 0, 0
        words = line.split(",")
        if type == "configure memory" and "configure resource" in line and len(words) >= words_size_2:
            x = int(words[1][6:]) / divider
            y = extract_config_mem(words, containerId)
        if type == "used memory" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider - 2
            y = extract_float(words, containerId)
        if type == "configure cpu" and "configure resource" in line and len(words) >= words_size_2:
            x = int(words[1][6:]) / divider
            y = extract_config_cpu(words, containerId)
        if type == "cpu usages" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "average cpu usage" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
            if y is not None and y  > 8:
                continue
        if type == "Instantaneous Delay" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "executors arrived" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_int(words, containerId)
        if type == "executors completed" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_int(words, containerId)
            # if y is not None and y > 70000:
            #     continue
        if type == "Arrival Rate" and "Processed Arrival Rate" not in line and type in line and len(words) >= words_size_1 and "Partition" not in line:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "Service Rate" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "executor heap used" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "executor non heap used" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "processing Rate" and "average processing Rate" not in line and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
            if y is not None and y > 200000:
                continue
        if type == "average processing Rate" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
            if y is not None and y > 100000:
                continue
        if type == "average arrival Rate" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
            # if y is not None and y > 100000:
            #     continue
        if type == "Processed Arrival Rate" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
            # if y is not None and y > 10000:
            #     continue
        if type == "executor pg major fault" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "Valid Rate" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "Max Major Fault" and type in line:
            x = int(words[1][6:]) / divider
            y = float(words[2][17:])
        if type == "page fault per cpu" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        if type == "pRate per cpu" and type in line and len(words) >= words_size_1:
            x = int(words[1][6:]) / divider
            y = extract_float(words, containerId)
        #
        # if x < 500 or x > 800:
        #     continue

        if x != 0 and y is not None and y >= 0:
            x_list.append(x)
            y_list.append(y)

    file.close()
    return x_list, y_list


def read_stat(file_name, type, containerId = "000002"):
    file = open(file_name)
    x_list, y_list = [], []
    for line in file.readlines():
        x = 0
        mem_stats = {}
        if type in line:
            words = line.split(": ")
            x = int(words[0].split(',')[1][6:]) / divider
            executor_list = words[1][1:-3].split("}, ")
            for executor in executor_list:
                map = executor.split('={')
                container = map[0]
                if container != containerId:
                    continue
                value = map[1].split(', ')
                for stat in value:
                    key = stat.split('=')[0]
                    value = int(stat.split('=')[1])
                    mem_stats[key] = value

        if x != 0 and len(mem_stats) > 0:
            x_list.append(x)
            y_list.append(mem_stats)

    return x_list, y_list


def get_pg_maj_fault(file_name, containerId = "000002"):
    time, value = read_stat(file_name, "executor mem stat", containerId)
    pgmajfault = exact_data(value, 'pgmajfault')
    if pgmajfault == None or len(pgmajfault) == 0:
        time, pgmajfault = read_file("executor pg major fault", file_name, containerId)
    return time, adjust_data(pgmajfault)


def get_stable_time(file_name, containerId = "000002"):
    time_list = []
    time, used_mem = read_file("used memory", file_name, containerId)
    time, config_mem = read_file("configure memory", file_name, containerId)
    first_index = 0
    for i in range(len(used_mem)):
        if config_mem[i] - used_mem[i] < 50:
            time_list.append(time[i])
            first_index = i
            break
    for i in range(first_index, len(time) - 1):
        if time[i+1] - time[i] > 60:
            time_list.append(time[i])
            time_list.append(time[i+1])
    time_list.append(time[-1])
    return time_list


def adjust_data(data):
    data.insert(0, 0)
    data = data[0:-1]
    return data


def get_next_file(root, cpu, mem):
    file_name = root+"/"+cpu+"/"+str(mem)+"M/stdout"
    try:
        f = open(file_name)
    except IOError:
        return None
    return file_name


def get_sub_stream(type, file_name):
    file = open(file_name)
    x_list, y_list = [], []
    for line in file.readlines():
        x, y = 0, 0
        words = line.split(",")
        if type in line:
            x = int(words[1][6:]) / divider
            y = int(words[2].split(":")[1][1:-1])

        if x != 0 and y is not None:
            x_list.append(x)
            y_list.append(y)

    file.close()
    return x_list, y_list


def calibrate(time, data):
    data_res = []
    t0 = 0
    d0 = 0
    for i in range(0, len(time)):
        if i > 0:
            t0 = int(time[i-1] * divider)
            d0 = data[i - 1]
        t1 = int(time[i] * divider)
        d1 = data[i]
        for j in range(t0+1, t1+1):
            d = (j-t0) * (d1 - d0) / (t1 - t0) + d0
            data_res.append(d)
    return data_res


def read_max_latency(file_name):
    time, latency = [], []
    file = open(file_name)
    for line in file.readlines():
        time.append(int(line.split(":")[0]))
        latency.append(int(line.split(":")[1][:-1]))
    return time, latency