package ucsc.cmps278.lambdaArch.heron;

import java.util.Map;
import java.util.Random;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Values;

public class TestWordSpout extends BaseRichSpout {

	private static final long serialVersionUID = -3217886193225455451L;
	private SpoutOutputCollector collector;
	private String[] words;
	private Random rand;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector acollector) {
		collector = acollector;
		words = new String[] { "nathan", "mike", "jackson", "golda", "bertels" };
		rand = new Random();
	}

	public void close() {
	}

	public void nextTuple() {
		final String word = words[rand.nextInt(words.length)];
		collector.emit(new Values(word));
	}

	public void ack(Object msgId) {
	}

	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}
}