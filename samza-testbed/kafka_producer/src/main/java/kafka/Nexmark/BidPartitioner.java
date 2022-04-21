package kafka.Nexmark;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.Map;

public class BidPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {
        int partitionNum = cluster.partitionCountForTopic(topic);
        int partition = 0;
        long id = (long) key;
        if (id > 0) {
            partition = (int) (id % partitionNum);
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
