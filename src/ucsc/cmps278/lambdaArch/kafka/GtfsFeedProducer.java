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

class Vehicle {
	private int seconds_since_report;
	private String run_id;
	private Double longitude;
	private float heading;
	private String route_id;
	private boolean predictable;
	private Double latitude;
	private String id;

	@Override
	public String toString() {
		return "{secondsSinceLastreport=" + seconds_since_report + ", runId=" + run_id + ", latitude="
				+ latitude + ", longitude=" + longitude + ", heading=" + heading + ", isPredictable=" + predictable
				+ ", routeId=" + id + "}";
	}

	public int getSecondsSinceReport() {
		return seconds_since_report;
	}

	public String getRunId() {
		return run_id;
	}

	public Double getLongitude() {
		return longitude;
	}

	public float getHeading() {
		return heading;
	}

	public String getRouteId() {
		return route_id;
	}

	public boolean isPredictable() {
		return predictable;
	}

	public Double getLatitude() {
		return latitude;
	}

}

class Vehicles {
	Vehicle[] items;

	@Override
	public String toString() {
		String res = null;

		for (Vehicle elem : items) {
			res += (elem);
		}
		return res;
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
		Vehicles item = new Gson().fromJson(res, Vehicles.class);
		StringBuffer output = new StringBuffer();
		
		output.append("[");
		for (Vehicle elem : item.items) {
			output.append("{lat:" + elem.getLatitude()).append(",lng:").append(elem.getLongitude() + "}"); 
			output.append(",");
		}
		output.append("{lat:34.1497, lng:-118.2799}]");
		
		return output.toString();
	}

	// Get GTFS feed
	public String getGTFSFeed(String url) {
		URL urlObj = null;
		try {
			urlObj = new URL(url);
		} catch (MalformedURLException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
		HttpURLConnection con = null;
		try {
			con = (HttpURLConnection) urlObj.openConnection();
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
