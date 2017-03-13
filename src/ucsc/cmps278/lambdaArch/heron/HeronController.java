package ucsc.cmps278.lambdaArch.heron;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class HeronController {

  private HeronController() { }

  /**
   * A spout that emits a random word
   */
  @SuppressWarnings("serial")
static class WordSpout extends BaseRichSpout {
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

  /**
   * A bolt that counts the words that it receives
   */
  @SuppressWarnings("serial")
static class ConsumerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> countMap;
    private int tupleCount;
    private String taskName;

    @SuppressWarnings("rawtypes")
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      collector = outputCollector;
      countMap = new HashMap<String, Integer>();
      tupleCount = 0;
      taskName = topologyContext.getThisComponentId() + "_" + topologyContext.getThisTaskId();
    }

	public void execute(Tuple tuple) {
		String key = tuple.getString(0);
		
		  System.out.print(tuple.getString(0) + " ");	
	      tupleCount += 1;
	      if (tupleCount % 100 == 0) {
	        tupleCount = 0;
	        System.out.println(taskName + " : " + Arrays.toString(countMap.entrySet().toArray()));
	      }

	      if (countMap.get(key) == null) {
	        countMap.put(key, 1);
	      } else {
	        Integer val = countMap.get(key);
	        countMap.put(key, ++val);
	      }

	      collector.ack(tuple);
	}
	
	@Override
	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new WordSpout(), 1);
    builder.setBolt("count", new ConsumerBolt(), 1).fieldsGrouping("word", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
