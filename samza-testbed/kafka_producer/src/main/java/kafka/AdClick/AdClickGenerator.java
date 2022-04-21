package kafka.AdClick;

public class AdClickGenerator {
    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String groupId = "test";
        String pageViewTopic = "pageview-join-input";
        String adClickTopic = "adclick-join-input";
        if (args != null && args.length == 3) {
            brokers = args[0];
            pageViewTopic = args[1];
            adClickTopic = args[2];
        }

        // Start group of User Consumer Thread
        PageViewThread pageViewProducer = new PageViewThread(brokers, groupId, pageViewTopic);
        AdClickThread adClickProducer = new AdClickThread(brokers, groupId, adClickTopic);

        Thread t1 = new Thread(pageViewProducer);
        t1.start();

        Thread t2 = new Thread(adClickProducer);
        t2.start();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
    }
}
