Root_Dir="$(dirname $(dirname $(pwd)))"
Hadoop_Dir=$Root_Dir/Hadoop-vScaling/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT
Tool_Dir=$Root_Dir/tools

$Tool_Dir/kafka/bin/kafka-server-stop.sh
python -c 'import time; time.sleep(5)'
$Tool_Dir/zookeeper/bin/zkServer.sh stop
python -c 'import time; time.sleep(5)'
rm -rf /tmp/kafka-logs/*
rm -rf $Tool_Dir/kafka/logs/*
rm nohup.out

$Tool_Dir/zookeeper/bin/zkServer.sh start
nohup $Tool_Dir/kafka/bin/kafka-server-start.sh $Tool_Dir/kafka/config/server.properties &

# ${Hadoop_Dir}/sbin/stop-dfs.sh
# rm -rf ${Hadoop_Dir}/dfs/*
# ${Hadoop_Dir}/sbin/stop-yarn.sh
 #${Hadoop_Dir}/sbin/start-yarn.sh
# ${Hadoop_Dir}/bin/hdfs namenode -format
# ${Hadoop_Dir}/sbin/hadoop-daemon.sh start namenode
# ${Hadoop_Dir}/sbin/hadoop-daemon.sh start datanode
# ${Hadoop_Dir}/sbin/hadoop-daemon.sh start secondarynamenode

python -c 'import time; time.sleep(5)'
