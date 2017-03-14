package ucsc.cmps278.lambdaArch.kafka;

import java.util.HashMap;
import java.util.Map.Entry;

public class GtfsFeedProcessor {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws InterruptedException {
		GtfsAppConfigurer config = new GtfsAppConfigurer();
		
		// Create Producer and Consumer
		GtfsFeedProducer producer = new GtfsFeedProducer("GTFS_FEED_ONE", config.configureKafkaProducer());
		GtfsFeedConsumer consumer = new GtfsFeedConsumer("GTFS_FEED_ONE", config.configureKafkaConsumer());

		// Publish
		HashMap<String, String> routes = new EndPoints().getRoutes();
		
		while(true) {
			Thread.sleep(1000);
			try {
				for (Entry<String, String> route : routes.entrySet()) {
					producer.produce(route.getValue());
				}
				
				//consumer.consume();
			} catch (Exception e) {
				System.out.println(e);
			} 
		}
	}
}
