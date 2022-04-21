package samzatask.AD;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.serializers.StringSerde;

import java.util.LinkedList;

public class LinkedListSerdeFactory implements SerdeFactory<LinkedList<String>> {
    public void OrderPoolSerdeFactory() {
    }

    public Serde<LinkedList<String>> getSerde(String name, Config config) {
        return new LinkedListSerde();
    }
}
