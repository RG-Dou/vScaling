#!/usr/bin/env bash

> result

params=(
    "1 4"
    "2 8"
    "4 16"
    "8 32"
    "16 60"
)

cd /home/drg/projects/work2/kubernetes/test/jdtests/scripts/

kubectl='/home/drg/projects/work2/kubernetes/cluster/kubectl.sh'

# 循环执行脚本
for param in "${params[@]}"
do

    pod_name=$($kubectl get pods | grep "hello-samza" | awk '{print $1}')
    bash patch-both1c.sh $pod_name hs1 1 4

    start_time=$(date +%s.%N)  # 记录开始时间
    bash patch-both1c.sh $pod_name hs1 $param


    # 获取 Pod 详细信息并检查资源限制和请求
    while true
    do
        pod_info=$($kubectl describe pod $pod_name)

        # 检查资源限制和请求
        cpu_limit=$(echo "$pod_info" | grep -A1 "Limits" | grep "cpu" | awk '{print $NF}')
        memory_limit=$(echo "$pod_info" | grep -A2 "Limits" | grep "memory" | awk '{print $NF}')
        cpu_request=$(echo "$pod_info" | grep -A1 "Requests" | grep "cpu" | awk '{print $NF}')
        memory_request=$(echo "$pod_info" | grep -A2 "Requests" | grep "memory" | awk '{print $NF}')

        echo "cpu limit: ${cpu_limit}; memory_limit: ${memory_limit}; cpu_request: ${cpu_request}; memory_request: ${memory_request}"

        if [[ "$cpu_limit" == "${param%% *}" && "$memory_limit" == "${param##* }Gi" && "$cpu_request" == "${param%% *}" && "$memory_request" == "${param##* }Gi" ]]; then
            echo "Pod $pod_name is correct."
            break
        else
            echo "Pod $pod_name is incorrect. Retrieving next pod..."
        fi
    done

    end_time=$(date +%s.%N)    # 记录结束时间
    duration=$(echo "$end_time - $start_time" | bc)
    echo "Execution time for param $param: $duration seconds"
    echo "Execution time for param $param: $duration seconds" >> result
done

#200000

#    # 获取 Pod 详细信息并检查资源限制和请求
#    while true
#    do
#        pod_info=$($kubectl describe pod $pod_name)
#
#        # 检查资源限制和请求
#        cpu_limit=$(echo "$pod_info" | grep -A1 "Limits" | grep "cpu" | awk '{print $NF}')
#        memory_limit=$(echo "$pod_info" | grep -A2 "Limits" | grep "memory" | awk '{print $NF}')
#        cpu_request=$(echo "$pod_info" | grep -A1 "Requests" | grep "cpu" | awk '{print $NF}')
#        memory_request=$(echo "$pod_info" | grep -A2 "Requests" | grep "memory" | awk '{print $NF}')
#
#        echo "cpu limit: ${cpu_limit}; memory_limit: ${memory_limit}; cpu_request: ${cpu_request}; memory_request: ${memory_request}"
#
#        if [[ "$cpu_limit" == "${param%% *}" && "$memory_limit" == "${param##* }Gi" && "$cpu_request" == "${param%% *}" && "$memory_request" == "${param##* }Gi" ]]; then
#            echo "Pod $pod_name is correct."
#            break
#        else
#            echo "Pod $pod_name is incorrect. Retrieving next pod..."
#        fi
#    done

#    # 获取 Pod 详细信息并检查资源限制和请求
#    while true
#    do
#        cpu_quota=$(cat /sys/fs/cgroup/cpu/kubepods/pod*/cpu.cfs_quota_us 2>/dev/null | grep -o '[0-9]*')
#        memory_limit=$(cat /sys/fs/cgroup/memory/kubepods/pod*/memory.limit_in_bytes 2>/dev/null)
#        echo "$((${param##* } * 1024 * 1024 * 1024))"
#        echo "$memory_limit"
#
#        if [[ "$cpu_quota" == "$((${param%% *} * 100000))" && "$memory_limit" == "$((${param##* } * 1024 * 1024 * 1024))" ]]; then
#            echo "Pod $pod_name is correct."
#            break
#        else
#            echo "Pod $pod_name is incorrect. Retrieving next pod..."
#        fi
#    done
