package ucsc.cmps278.lambdaArch;

import java.util.HashMap;
import java.util.Properties;

public class GtfsFeedProcessor {

	public static void main(String[] args) {
		// Producer
		GtfsFeedProducer producer = new GtfsFeedProducer("GTFS_FEED_ONE", configure());
		
		// SEND message list
		HashMap<String, String> msgList = new HashMap<String, String>();
		msgList.put("le home", "Santa Cruz, CA");
		msgList.put("le office", "San Jose, CA");
		msgList.put("le Indian food!!", "Santa Clara, CA");
		
		// Publish
		producer.produce(msgList);
	}
	
	static Properties configure() {
		// properties setup
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("acks", "all");
		prop.put("retires", 0);
		prop.put("batch.size", 20000);
		prop.put("linger.ms", 1);
		prop.put("buffer.memory", Integer.MAX_VALUE);
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	
		return prop;
	}
}



