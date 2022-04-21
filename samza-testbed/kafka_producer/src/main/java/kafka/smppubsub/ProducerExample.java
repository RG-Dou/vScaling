package kafka.smppubsub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SSE generaor
 */
public class ProducerExample {

	private String TOPIC;

	private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

	public ProducerExample(String input) {
		TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);

    }
    
    public void generate(String file, int speed) throws InterruptedException {

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null; 
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        long interval = 0;
        int counter = 0;
        try {
			stream = new FileReader("/root/SSE-kafka-producer/"+file+".txt");
            br = new BufferedReader(stream);
            interval = 1000000000/1;
            start = System.nanoTime();

            while ((sCurrentLine = br.readLine()) != null) {

                cur = System.nanoTime();
                if (sCurrentLine.equals("end")) {
                    continue;
                }

                if (sCurrentLine.split("\\|").length < 10) {
                    continue;
                }

                String msg = String.valueOf(System.currentTimeMillis()) + "," +
                        "c21234bcbf1e8eb4c61f1927190efebd,Splitter FlatMap-0,1,979.6359306643908,9979.478178259418," +
                        "978.0266121680536,9963.084170737484,2141.713,507923,5160882,0.998357227980352,0:39770&1:39224&2:39711&3:" +
                        "39162&4:40317&5:39725&6:40135&7:40265&8:39570&9:39809&10:39743&11:39310&12:40573&13:39147&14:40452&15:410" +
                        "96&16:40320&17:40392&18:39901&19:40112&20:40629&21:39571&22:39490&23:38574&24:39605&25:39989&26:39178&27:3" +
                        "8921&28:41292&29:39291&30:39552&31:40021&32:37788&33:40115&34:39213&35:39713&36:40876&37:40268&38:40277&39:3" +
                        "9102&40:40532&41:39037&42:39417&43:39320&44:40742&45:39585&46:40201&47:40332&48:41001&49:41829&50:40379&51:393" +
                        "47&52:40441&53:39450&54:39539&55:41673&56:38247&57:39818&58:39593&59:41511&60:40114&61:39193&62:41422&63:39914&64" +
                        ":41958&65:39821&66:92543&67:39508&68:40408&69:39843&70:39571&71:39455&72:39984&73:40384&74:39410&75:40297&76:39457&7" +
                        "7:39781&78:41060&79:39840&80:38445&81:39592&82:41131&83:39724&84:39989&85:39103&86:39852&87:39438&88:39560&89:38440&9" +
                        "0:39313&91:39900&92:41250&93:40252&94:39363&95:38427&96:39497&97:41289&98:39759&99:41038&100:39316&101:41172&102:40551" +
                        "&103:39032&104:39835&105:40826&106:39885&107:39206&108:39310&109:40970&110:37903&111:38424&112:38718&113:40838&114:412" +
                        "65&115:40197&116:39988&117:40493&118:40440&119:39338&120:40168&121:38392&122:39350&123:41948&124:39152&125:40706&126:404" +
                        "15&127:39526,0:1301&1:757&2:6187&3:480&4:770&5:942&6:1159&7:7203&8:296&9:1160&10:1050&11:12758&12:5796&13:8327&14:8347&15" +
                        ":7852&16:9295&17:10959&18:9356&19:9528&20:6113&21:8512&22:7684&23:6070&24:10808&25:9181&26:7580&27:774&28:621&29:660&30:837" +
                        "&31:721&32:735&33:597&34:1216&35:621&36:596&37:634&38:672&39:671&40:767&41:786&42:514&43:1230&44:1021&45:1175&46:894&47:100" +
                        "0&48:1002&49:1761&50:245&51:576&52:710&53:525&54:1124&55:657&56:571&57:609&58:1138&59:493&60:526&61:1004&62:11359&63:10782&64" +
                        ":9296&65:10534&66:9584&67:7211&68:4592&69:8411&70:717&71:1111&72:1188&73:776&74:844&75:1388&76:652&77:676&78:337&79:989&80:1" +
                        "063&81:1047&82:684&83:576&84:882&85:736&86:1049&87:860&88:1186&89:557&90:867&91:772&92:1415&93:812&94:506&95:958&96:1144&97:" +
                        "942&98:957&99:1164&100:819&101:717&102:7773&103:10910&104:7749&105:6775&106:5809&107:6596&108:9169&109:9509&110:10766&111:76" +
                        "02&112:10602&113:8003&114:12367&115:5615&116:10810&117:10252&118:4242&119:8253&120:11313&121:5362&122:7874&123:7776&124:7591" +
                        "&125:6767&126:11179&127:9945,1580908351767";

                ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, sCurrentLine.split("\\|")[Sec_Code],
                        msg);
                producer.send(newRecord);
                counter++;

                while ((System.nanoTime() - cur) < interval) {}
                if (System.nanoTime() - start >= 1000000000) {
                    System.out.println("output rate: " + counter);
                    counter = 0;
                    start = System.nanoTime();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(stream != null) stream.close();
                if(br != null) br.close();
            } catch(IOException ex) {
                ex.printStackTrace();
            }
        }
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
		String TOPIC = new String("test");
        String file = new String("partition1");
        int speed = 1;
        if (args.length > 0) {
			TOPIC = args[0];
        	file = args[1];
        	speed = Integer.parseInt(args[2]);
		}
		new ProducerExample(TOPIC).generate(file, speed);
    }
}
