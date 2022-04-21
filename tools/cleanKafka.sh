~/tools/kafka/bin/kafka-server-stop.sh

python -c 'import time; time.sleep(5)'
rm -rf /data/drg_data/kafka-logs/*
rm -rf ~/tools/kafka/logs/*
rm nohup.out

nohup ~/tools/kafka/bin/kafka-server-start.sh ~/tools/kafka/config/server.properties &

Hadoop_Dir="/home/drg/projects/work2/hadoop-dir/Hadoop-YarnVerticalScaling"
${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/sbin/stop-dfs.sh
rm -rf ${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/dfs/*
${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/bin/hdfs namenode -format
${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/sbin/hadoop-daemon.sh start namenode
${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/sbin/hadoop-daemon.sh start datanode
${Hadoop_Dir}/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/sbin/hadoop-daemon.sh start secondarynamenode

python -c 'import time; time.sleep(5)'
