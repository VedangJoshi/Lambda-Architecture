package ucsc.cmps278.lambdaArch;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class GtfsFeedProcessor {

	public static void main(String[] args) {
		
		// Topic Name 
		String topic = "GTFS_FEED_ONE";
		
		// Properties
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("acks", "all");
		prop.put("retires", 0);
		prop.put("batch.size", 20000);
		prop.put("linger.ms", 1);
		prop.put("buffer.memory", Integer.MAX_VALUE);
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// Producer
		Producer<String, String> producer = new KafkaProducer<String, String>(prop); 
		
		for (int i = 0; i < 5; i++) {
			producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
		}
		
		System.out.println("Msg sent!");
		producer.close();
	}
}



