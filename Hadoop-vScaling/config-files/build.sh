# cd target
# tar -zxvf hadoop-3.0.0-SNAPSHOT.tar.gz
# cd ..

# cp r_files/container-executor target/hadoop-3.0.0-SNAPSHOT/bin/
# sudo chown root:drg target/hadoop-3.0.0-SNAPSHOT/bin/container-executor
# sudo chmod 6050 target/hadoop-3.0.0-SNAPSHOT/bin/container-executor

hadoop=../hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/etc/hadoop

cp hadoop-env.sh $hadoop/
cp yarn-site.xml $hadoop/
cp capacity-scheduler.xml $hadoop/
cp container-executor.cfg $hadoop/
cp slaves $hadoop/
cp core-site.xml $hadoop/
cp hdfs-site.xml $hadoop/
