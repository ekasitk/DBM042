import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SampleLogKafkaProducer {
  public static void main(String[] args) {
    try {
      if (args.length != 3) {
        System.out.println("Usage - java SampleLogKafkaProducer <input> <broker> <topic>");
        System.out.println("    broker is a comma-separated list of host:port, e.g. localhost:9092");
        System.exit(0);
      }
      File read = new File(args[0]);
      BufferedReader reader = new BufferedReader(new FileReader(read));

      Properties properties = new Properties();
      properties.put("metadata.broker.list", args[1]);
      properties.put("serializer.class", "kafka.serializer.StringEncoder");
      properties.put("request.required.acks", "1");
      ProducerConfig config = new ProducerConfig(properties);

      Producer<String,String> producer = new Producer<String,String>(config);

      String topic = args[2];
      for (;;) {
        String line = reader.readLine();
        System.out.println(line);
        // Set Key: String and Value: String, but we ignore Key.
        KeyedMessage<String,String> message = new KeyedMessage<String,String>(topic,line);
        producer.send(message);
        Thread.sleep(200);
      }
    } catch (Exception e) {                                    
      e.printStackTrace();                                     
    }      
  }
}
