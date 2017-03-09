package ucsc.cmps278.lambdaArch;

public class GtfsFeedProcessor {

	public static void main(String[] args) {
		GtfsAppConfigurer config = new GtfsAppConfigurer();
		
		// Create Producer and Consumer
		GtfsFeedProducer producer = new GtfsFeedProducer("GTFS_FEED_ONE", config.configureKafkaConsumer());
		GtfsFeedConsumer consumer = new GtfsFeedConsumer("GTFS_FEED_ONE", config.configureKafkaConsumer());

		// Publish - Subcribe
		String url = config.getEndPointURI("Route_704");
		System.out.println("Sending 'GET' request to Producer via URL : " + url);
		
		try {
			producer.produce(url);
			consumer.consume();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
