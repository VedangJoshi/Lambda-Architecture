package ucsc.cmps278.lambdaArch;

import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class GtfsFeedProducer {
	// Topic Name
	String topic;

	// Producer
	Producer<String, String> producer;

	// Ctor
	public GtfsFeedProducer(String topic, Properties prop) {
		this.topic = topic;
		this.producer = new KafkaProducer<String, String>(prop);
	}

	// Publish records
	void produce(HashMap<String, String> msgList) {
		for (String key : msgList.keySet()) {
			producer.send(new ProducerRecord<String, String>(topic, key, msgList.get(key)));
		}
		producer.close();
	}
	
	void produce(String s) {
		producer.send(new ProducerRecord<String, String>(topic, s));
		producer.close();
	}
}
