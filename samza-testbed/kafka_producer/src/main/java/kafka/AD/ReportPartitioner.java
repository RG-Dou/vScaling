package kafka.AD;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.Map;

public class ReportPartitioner implements Partitioner {

    private HashMap<Integer, Integer> stockToPartition = new HashMap();
    private int leftPartition = 0;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {
        int partitionNum = cluster.partitionCountForTopic(topic);
        int partition = 0;
        long stockId = (long) key;
        if (stockId > 0) {
            partition = (int) (stockId % partitionNum);
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
