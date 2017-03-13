package ucsc.cmps278.lambdaArch.heron;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * A spout that emits a random word
 */
@SuppressWarnings("serial")
public class Spout extends BaseRichSpout {
  private Random rnd;
  private SpoutOutputCollector collector;
  
	public void nextTuple() {
		String[] list = {"Jack", "Mary", "Jill", "McDonald", "ABC", "PQR"};
	      Utils.sleep(10);
	      int nextInt = rnd.nextInt(list.length);
	      collector.emit(new Values(list[nextInt]));
	}

	public void open(Map<String, Object> arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		rnd = new Random(31);
	      collector = arg2;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("word"));
	}
}