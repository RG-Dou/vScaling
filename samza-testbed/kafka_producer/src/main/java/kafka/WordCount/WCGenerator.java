package kafka.WordCount;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * SSE generaor
 */
public class WCGenerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    private long SENTENCE_NUM = 1000000000;
    private int uniformSize = 10000;
    private double mu = 10;
    private double sigma = 1;

    String TimeSeparator = "|";

    public WCGenerator(String input) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", "alligator:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public void generate(int speed) throws InterruptedException {
        RandomDataGenerator messageGenerator = new RandomDataGenerator();
        long time = System.currentTimeMillis();
        long interval = 1000000000/5000;
        long cur = 0;

        long start = System.nanoTime();
        int counter = 0;
        // for loop to generate message
        for (long sent_sentences = 0; sent_sentences < SENTENCE_NUM; ++sent_sentences) {
            cur = System.nanoTime();

            double sentence_length = messageGenerator.nextGaussian(mu, sigma);
            StringBuilder messageBuilder = new StringBuilder();
            for (int l = 0; l < sentence_length; ++l) {
                int number = messageGenerator.nextInt(1, uniformSize);
                messageBuilder.append(String.valueOf(number)).append(" ");
            }

            ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, messageBuilder.toString());
            producer.send(newRecord);

            counter++;
            // control data generate speed
            while ((System.nanoTime() - cur) < interval) {}
            if (System.nanoTime() - start >= 1000000000) {
                System.out.println("output rate: " + counter);
                counter = 0;
                start = System.nanoTime();
            }
        }
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        String TOPIC = new String("WordCount");
        int speed = 1;
        if (args.length > 0) {
            TOPIC = args[0];
            speed = Integer.parseInt(args[2]);
        }
        new WCGenerator(TOPIC).generate(speed);
    }
}

