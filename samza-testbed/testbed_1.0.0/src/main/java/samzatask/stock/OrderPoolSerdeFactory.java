package samzatask.stock;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.serializers.StringSerde;

import java.util.ArrayList;
import java.util.HashMap;

public class OrderPoolSerdeFactory implements SerdeFactory<HashMap<Integer, ArrayList<Order>>> {
    public OrderPoolSerdeFactory() {
    }

    public Serde<HashMap<Integer, ArrayList<Order>>> getSerde(String name, Config config) {
        return new OrderPoolSerde();
    }
}
