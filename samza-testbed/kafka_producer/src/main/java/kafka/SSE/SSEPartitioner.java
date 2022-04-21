package kafka.SSE;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.Map;

public class SSEPartitioner implements Partitioner {

    private HashMap<Integer, Integer> stockToPartition = new HashMap();
    private int leftPartition = 0;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {
        int partitionNum = cluster.partitionCountForTopic(topic);
        int partition = 0;
        String combinedKey = (String) key;
        int stockId = Integer.parseInt(combinedKey.split("\\|")[0]);
        if (stockId > 0) {
            partition = stockId % partitionNum;
        }
//        if (!stockToPartition.containsKey(stockId)) {
////            System.out.println(String.format("map stockId %d to %d", stockId, leftPartition));
//            stockToPartition.put(stockId, leftPartition);
//            leftPartition++;
//        }
//        partition = stockToPartition.get(stockId);
//        if (partition > partitionNum) {
////            System.out.println(String.format("+++Wrong...partition: %d, max partition: %d", partition, partitionNum));
//            partition = stockId % partitionNum;
//        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
