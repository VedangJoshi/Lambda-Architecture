package ucsc.cmps278.lambdaArch.heron;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import ucsc.cmps278.lambdaArch.kafka.GtfsAppConfigurer;
import ucsc.cmps278.lambdaArch.kafka.GtfsFeedConsumer;

/**
 * A spout that emits a random word
 */
@SuppressWarnings("serial")
public class Spout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private GtfsAppConfigurer config;
	private GtfsFeedConsumer consumer;
	
	public void nextTuple() {
		config = new GtfsAppConfigurer();
		consumer = new GtfsFeedConsumer("GTFS_FEED_ONE", config.configureKafkaConsumer());
		
		ArrayList<String> list = null;

		try {
			list = consumer.consume();
		} catch (Exception e) {
			e.printStackTrace();
		}

		for(int i = 0; i < list.size(); i++) {
			collector.emit(new Values(list.get(i)));
		}
	}

	public void open(Map<String, Object> arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		collector = arg2;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("word"));
	}
}