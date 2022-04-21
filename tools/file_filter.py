import os
import re
from shutil import copyfile

root_dir = "/data/drg_data/results/"

def get_app_name(file):
    file = open(file)
    for line in file.readlines():
        if "system config" in line:
            app_name = re.findall("nexmark-q\d", line)
            return app_name[0]

def get_app_attri(file):
    file = open(file)
    for line in file.readlines():
        if "executor configure resource" in line:
            memory = re.findall("memory:\d+", line)[0]
            cores = re.findall("vCores:\d+", line)[0]
            return memory+"+"+cores

if __name__ == "__main__":
    for root1, dirs1, files1 in os.walk(root_dir):
        for dir1 in dirs1:
            for root2, dirs2, files2 in os.walk(root1+"/"+dir1):
                for dir2 in dirs2:
                    if dir2[-6:] == "000001":
                        for root3, dirs3, files3 in os.walk(root2 + "/" + dir2):
                            copy_name = ""
                            copy_file = ""
                            flag = True
                            for file3 in files3:
                                if file3 == "samza-job-coordinator.log":
                                    bench = get_app_name(root3+"/"+file3)
                                    copy_name += "+" + bench
                                if file3 == "stdout":
                                    attributes = get_app_attri(root3+"/"+file3)
                                    if attributes ==None:
                                        flag = False
                                        break
                                    copy_name += "+"+attributes
                                    copy_file = root3+"/"+file3
                            if flag:
                                copyfile(copy_file, root_dir+"/sum/"+copy_name)
