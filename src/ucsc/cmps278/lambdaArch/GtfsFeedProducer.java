package ucsc.cmps278.lambdaArch;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
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
	void produce(String url) {
		String res = null;
		try {
			res = getGTFSFeed(url);
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.send(new ProducerRecord<String, String>(topic, res));
		producer.close();
	}

	public String getGTFSFeed(String url) throws Exception {
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
