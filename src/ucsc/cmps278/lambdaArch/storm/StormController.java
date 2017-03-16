package ucsc.cmps278.lambdaArch.storm;

import java.util.HashMap;

import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class StormController {

	private StormController() {
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 4) {
			System.exit(1);
		}
		
		final ZkHosts zkrHosts = new ZkHosts(args[0]);
		final String kafkaTopic = args[1];
		final String zkRoot = args[2];
		final String clientId = args[3];
		final SpoutConfig kafkaConf = new SpoutConfig(zkrHosts, kafkaTopic, zkRoot, clientId);
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("kafka-spout", new KafkaSpout(kafkaConf), 1);
		topologyBuilder.setBolt("print-messages", new Bolt()).globalGrouping("kafka-spout");
		
		LocalCluster localCluster = new LocalCluster();
		
		localCluster.submitTopology("kafka-topology", new HashMap<>(), topologyBuilder.createTopology());
		Thread.sleep(3000);
	}
}
