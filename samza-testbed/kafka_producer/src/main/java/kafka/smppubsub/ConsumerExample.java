package kafka.smppubsub;

public class ConsumerExample {

    public static void main(String[] args) {

        String brokers = "localhost:9092";
        String groupId = "test";
        String topic = "bids";

        if (args != null && args.length == 3) {
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
        }

        // Start group of User Consumer Thread
        UserConsumerThread consumerThread = new UserConsumerThread(brokers, groupId, topic);
        Thread t2 = new Thread(consumerThread);
        t2.start();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
    }
}