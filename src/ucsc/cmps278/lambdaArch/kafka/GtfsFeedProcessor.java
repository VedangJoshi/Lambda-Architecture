package ucsc.cmps278.lambdaArch.kafka;

public class GtfsFeedProcessor {

	public static void main(String[] args) throws InterruptedException {
		GtfsAppConfigurer config = new GtfsAppConfigurer();
		
		// Create Producer and Consumer
		GtfsFeedProducer producer = new GtfsFeedProducer("GTFS_FEED_ONE", config.configureKafkaProducer());
		GtfsFeedConsumer consumer = new GtfsFeedConsumer("GTFS_FEED_ONE", config.configureKafkaConsumer());

		// Publish - Subcribe
		String url = config.getEndPointURI("Route_704");
		System.out.println("Sending 'GET' request to Producer via URL : " + url);
		
		while(true) {
			Thread.sleep(1000);
			try {
				producer.produce(url); 
				consumer.consume();
			} catch (Exception e) {
				System.out.println(e);
			} 
		}
	}
}
