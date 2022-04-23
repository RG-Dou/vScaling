~/tools/kafka/bin/kafka-server-stop.sh

Root_Dir="$(dirname $(pwd))"
Hadoop_Dir=$Root_Dir/Hadoop-vScaling/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT
Tool_Dir=$Root_Dir/tools

python -c 'import time; time.sleep(5)'
rm -rf /temp/*
rm -rf $Tool_Dir/kafka/logs/*
rm nohup.out

nohup $Tool_Dir/kafka/bin/kafka-server-start.sh $Tool_Dir/kafka/config/server.properties &

${Hadoop_Dir}/sbin/stop-dfs.sh
rm -rf ${Hadoop_Dir}/dfs/*
${Hadoop_Dir}/bin/hdfs namenode -format
${Hadoop_Dir}/sbin/hadoop-daemon.sh start namenode
${Hadoop_Dir}/sbin/hadoop-daemon.sh start datanode
${Hadoop_Dir}/sbin/hadoop-daemon.sh start secondarynamenode

python -c 'import time; time.sleep(5)'
