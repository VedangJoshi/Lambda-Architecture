package ucsc.cmps278.lambdaArch.kafka;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;

class BusStop {
	Double latitude;
	String display_name;
	String id;
	Double longitude;
	
	@Override
	public String toString() {
		return "latitude = " + latitude + ", display_name = " + display_name + ", "
							+ "id = " + id + ", longitude = " + longitude + "]";
	}
}

class Item {
	BusStop[] items;

	@Override
	public String toString() {
		String res = null;
		
		for (BusStop busStop : items) {
			res += busStop.toString();
		}
		return "";
	}
}


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
	public void produce(String url) {
		String res = null;
		try {
			res = getGTFSFeed(url);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		producer.send(new ProducerRecord<String, String>(topic, jsonToString(res)));
	}

	// Convert API response to string
	private String jsonToString(String res) {
		Item item = new Gson().fromJson(res, Item.class);
		String out = "";
		
		for (BusStop elem : item.items) {
			out += elem.toString();
		}
		return out;
	}
	
	// Get GTFS feed
	public String getGTFSFeed(String url) {
		URL obj = null;
		try {
			obj = new URL(url);
		} catch (MalformedURLException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
		HttpURLConnection con = null;
		try {
			con = (HttpURLConnection) obj.openConnection();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
		try {
			con.setRequestMethod("GET");
		} catch (ProtocolException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}

		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
		String inputLine;
		StringBuffer response = new StringBuffer();

		try {
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			
			in.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}

		return response.toString();
	}
}
