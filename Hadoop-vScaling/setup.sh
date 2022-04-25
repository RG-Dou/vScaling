user=$1

# mvn clean package -Pdist -Pdoc -Psrc -Ptar -DskipTests -e

sudo mkdir /etc/hadoop/
sudo cp config-files/container-executor.cfg /etc/hadoop/

cd hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager
mvn package -Pdist,native -DskipTests -Dtar -Dcontainer-executor.conf.dir=/etc/hadoop
sudo cp target/native/target/usr/local/bin/container-executor ../../../../hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/bin/
cd ../../../../hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/
sudo chown root:$user bin/container-executor
sudo chmod 6050 bin/container-executor

cd ../../../config-files
sh build.sh

cd ..
mvn install -DskipTests

cgroup=/sys/fs/cgroup
sudo mkdir $cgroup/blkio/yarn/
sudo chown -R $user:$user $cgroup/blkio/yarn
sudo mkdir $cgroup/cpu/yarn/
sudo chown -R $user:$user $cgroup/cpu/yarn
sudo mkdir $cgroup/memory/yarn/
sudo chown -R $user:$user $cgroup/memory/yarn
sudo mkdir $cgroup/net_cls/yarn/
sudo chown -R $user:$user $cgroup/net_cls/yarn

cd hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/
./sbin/stop-yarn.sh
./sbin/start-yarn.sh
python -c 'import time; time.sleep(5)'
rm -rf dfs/*
./bin/hdfs namenode -format
./sbin/hadoop-daemon.sh start namenode
./sbin/hadoop-daemon.sh start datanode
./sbin/hadoop-daemon.sh start secondarynamenode
