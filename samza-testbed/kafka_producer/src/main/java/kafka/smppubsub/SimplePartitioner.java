package kafka.smppubsub;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * stockid mod partition number, simple hash function
 */
public class SimplePartitioner implements Partitioner {

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
						 Cluster cluster) {
		int partitionNum = cluster.partitionCountForTopic(topic);
		int partition = 0;
		String combinedKey = (String) key;
		int stockId = Integer.parseInt(combinedKey);
		if (stockId > 0) {
			partition = stockId % partitionNum;
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