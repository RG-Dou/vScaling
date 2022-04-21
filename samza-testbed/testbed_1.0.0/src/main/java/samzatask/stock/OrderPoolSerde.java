package samzatask.stock;

import org.apache.samza.serializers.Serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Author by Mao
 * kmeans data structure, and some operator
 */

class OrderPoolSerde implements Serde<HashMap<Integer, ArrayList<Order>>> {
    public OrderPoolSerde() {
    }

    @Override
    public HashMap<Integer, ArrayList<Order>> fromBytes(byte[] bytes) {
        try {
            ByteArrayInputStream bias = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bias);
            HashMap<Integer, ArrayList<Order>> orderPool;
            orderPool = (HashMap<Integer, ArrayList<Order>>) ois.readObject();
            return orderPool;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] toBytes(HashMap<Integer, ArrayList<Order>> orderPool) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream dos = new ObjectOutputStream(baos);
            dos.writeObject(orderPool);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


