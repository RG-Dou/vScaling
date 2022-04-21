package kafka.SSE;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.Map;

public class SSERRPartitioner implements Partitioner {

    private HashMap<Integer, Integer> stockToPartition = new HashMap();
    private int round = 0;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {
        int partitionNum = cluster.partitionCountForTopic(topic);
        int partition = 0;
        round++;
        return round%partitionNum;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
