import sys
import os


def get_app_path(path):
    for root1, dirs1, files1 in os.walk(path):
        for sub_dir in dirs1:
            return sub_dir


def get_am_path(path):
    for root1, dirs1, files1 in os.walk(path):
        for sub_dir in dirs1:
            if sub_dir[-6:] == "000001":
                return sub_dir


def get_message(file):
    file = open(file)
    end_time, schedule_time = 0, 0
    for line in file.readlines():
        if "Model, time" in line:
            end_time = int(line.split(" ")[2])
        if "last time" in line:
            schedule_time = end_time
    if end_time < 18000:
        print("error")
        return
    if end_time - schedule_time > 1000:
        print("error")
        return
    print("good")


def check_error(root):
    file = root + '/' + get_app_path(root) + '/' + get_am_path(root) + "/stdout"
    get_message(file)


if __name__ == "__main__":
    root = sys.argv[1]
    check_error(root)