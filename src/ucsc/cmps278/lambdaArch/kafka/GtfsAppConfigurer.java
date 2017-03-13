package ucsc.cmps278.lambdaArch.kafka;

import java.util.Properties;

public class GtfsAppConfigurer {
	
	// Properties
	Properties prop;
	
	// Endpoints
	EndPoints routesURI;
	
	// Ctor
	public GtfsAppConfigurer() {
		this.routesURI = new EndPoints();
	}
	
	public Properties configureKafkaProducer() {
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

	public Properties configureKafkaConsumer() {
		// properties setup
		Properties prop = new Properties();
		prop.put("bootstrap.servers", "localhost:9092");
		prop.put("group.id", "test");
		prop.put("enable.auto.commit", "true");
		prop.put("auto.commit.interval.ms", "1000");
		prop.put("session.timeout.ms", "30000");
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// For logs from beginning
		prop.put("auto.offset.reset", "earliest");

		return prop;
	}
	
	void setEndPointURI(String route, String URI) {
		routesURI.setRoutes(route, URI);
	}
}
