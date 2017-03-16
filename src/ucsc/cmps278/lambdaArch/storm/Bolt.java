package ucsc.cmps278.lambdaArch.storm;

import java.util.Map;

@SuppressWarnings("serial")
public class Bolt implements backtype.storm.topology.IRichBolt {
	private backtype.storm.task.OutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void prepare(Map map, backtype.storm.task.TopologyContext topologyContext, backtype.storm.task.OutputCollector outputCollector) {
		collector = outputCollector;
	}

	@Override
	public void execute(backtype.storm.tuple.Tuple tuple) {
		if(Integer.parseInt(tuple.getString(0)) > 108) {
			System.err.println("Error -> " + tuple.getString(0));
			collector.ack(tuple);
			return;
		}
		
		System.out.println("Average_Report_Time: " + tuple.getString(0) + " ");
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer arg0) {
	}
}
