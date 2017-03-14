package ucsc.cmps278.lambdaArch.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class GtfsFeedConsumer {

	String topic;
	Consumer<String, String> consumer;

	public GtfsFeedConsumer(String topic, Properties prop) {
		this.topic = topic;
		this.consumer = new KafkaConsumer<String, String>(prop);
		consumer.subscribe(Arrays.asList(topic));
	}

	public ArrayList<String> consume() throws Exception {
		ConsumerRecords<String, String> records = consumer.poll(100);
		ArrayList<String> values = new ArrayList<>();
		
		System.out.println("Number of records: " + records.count());
		for (ConsumerRecord<String, String> record : records) {
			//System.out.println("offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
			values.add(record.value());
		}
		
		System.out.println("loop");
		return values;
	}
}
