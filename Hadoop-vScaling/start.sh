cd hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/
./sbin/stop-dfs.sh
rm -rf dfs/*
./sbin/stop-yarn.sh
./sbin/start-yarn.sh
python -c 'import time; time.sleep(5)'
./bin/hdfs namenode -format
./sbin/hadoop-daemon.sh start namenode
./sbin/hadoop-daemon.sh start datanode
./sbin/hadoop-daemon.sh start secondarynamenode

