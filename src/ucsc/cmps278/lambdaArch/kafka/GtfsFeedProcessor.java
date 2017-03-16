package ucsc.cmps278.lambdaArch.kafka;

import java.util.HashMap;
import java.util.Map.Entry;

public class GtfsFeedProcessor {

	public static void main(String[] args) throws InterruptedException {
		GtfsAppConfigurer config = new GtfsAppConfigurer();
		
		// Create Producer and Consumer
		GtfsFeedProducer producer = new GtfsFeedProducer("GTFS_FEED_ONE", config.configureKafkaProducer());

		// Read end points
		HashMap<String, String> routes = new EndPoints().getRoutes();
		
		// Publish
		while(true) {
			try {
				for (Entry<String, String> route : routes.entrySet()) {
					producer.produce(route.getValue());
				}
				
			} catch (Exception e) {
				System.out.println(e);
			} 
		}
	}
}
