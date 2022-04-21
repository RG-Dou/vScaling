package samzatask.AD;

import org.apache.samza.serializers.Serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

/**
 * Author by DrG
 * kmeans data structure, and some operator
 */

class LinkedListSerde implements Serde<LinkedList<String>> {
    public LinkedListSerde() {
    }

    @Override
    public LinkedList<String> fromBytes(byte[] bytes) {
        try {
            ByteArrayInputStream bias = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bias);
            LinkedList<String> orderPool;
            orderPool = (LinkedList<String>) ois.readObject();
            return orderPool;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] toBytes(LinkedList<String> list) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream dos = new ObjectOutputStream(baos);
            dos.writeObject(list);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


