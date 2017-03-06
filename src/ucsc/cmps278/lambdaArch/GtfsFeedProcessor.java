package ucsc.cmps278.lambdaArch;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Properties;

public class GtfsFeedProcessor {

	public static void main(String[] args){
		
		GtfsFeedProducer producer = new GtfsFeedProducer("GTFS_FEED_ONE", configureForProducer());
		GtfsFeedConsumer consumer = new GtfsFeedConsumer("GTFS_FEED_ONE", configureForConsumer());
		
//		// SEND message list
//		HashMap<String, String> msgList = new HashMap<String, String>();
//		msgList.put("le home", "Santa Cruz, CA");
//		msgList.put("le office", "San Jose, CA");
//		msgList.put("le Indian food!!", "Santa Clara, CA");
//		
//		// Publish
//		producer.produce(msgList);
		
		// Publish
		String url = "http://api.metro.net/agencies/lametro/routes/704/vehicles/";
		try
		{
			System.out.println("Sending 'GET' request to Producer via URL : " + url);
			producer.produce(makeGetRequest(url));
			System.out.println("Subscribing via Consumer: ");
			consumer.consume();
		}
		
		catch (Exception e)
		{
			System.out.println(e);
		}
	}
	
	static Properties configureForProducer() {
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
	
	static Properties configureForConsumer() {
		// properties setup
		Properties prop = new Properties();
	    prop.put("bootstrap.servers", "localhost:9092");
	    prop.put("group.id", "test");
	    prop.put("enable.auto.commit", "true");
	    prop.put("auto.commit.interval.ms", "1000");
	    prop.put("session.timeout.ms", "30000");
	    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    
	    //For logs from beginning 
	    prop.put("auto.offset.reset", "earliest");
		
	    return prop;
	}
	
	public static String makeGetRequest(String url) throws Exception
	{
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("GET");

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		return response.toString();
	}
}



