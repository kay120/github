package com.kay;

import org.apache.log4j.Logger;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import com.kay.util.StormRunner;

import com.kay.bolt.RollingCountBolt;
import com.kay.bolt.SpliterBolt;
import com.kay.bolt.WriterBolt;
import com.kay.spout.MessageScheme;

import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class ProcessLogTopology {
	private static final Logger LOG = Logger.getLogger(ProcessLogTopology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	
	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;
	/**
	 * @param topologyName
	 */
	public ProcessLogTopology(String topologyName) throws InterruptedException{
		builder = new TopologyBuilder();
		this.topologyName = topologyName;
		this.topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		wireTopology();
	}

	private void wireTopology() throws InterruptedException{
		String spoutId = "kafkaspout";
		String spliterId = "spliter";
		String counterId = "counter";
		String writerId = "writer";
		String topic = "logkafka";
		String zkRoot = "/storm";
		BrokerHosts brokerHosts = new ZkHosts("master1:2181,master2:2181,slave1:2181"); 
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic , zkRoot, spoutId);
		spoutConfig.forceFromStart = true;
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		builder.setSpout(spoutId, new KafkaSpout(spoutConfig));
		builder.setBolt(spliterId, new SpliterBolt()).shuffleGrouping(spoutId);
		builder.setBolt(counterId, new RollingCountBolt(5,1)).fieldsGrouping(spliterId, new Fields("accessip_serverip"));
//		builder.setBolt(writerId, new WriterBolt()).shuffleGrouping(counterId);		
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}
	
	public void runLocally() throws InterruptedException {
		System.err.println("runLocally");
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
	}

	public void runRemotely() throws Exception {
		StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
	}
		  
	/**
	 * Submits (runs) the topology.
	 * Usage: "RollingTopWords [topology-name] [local|remote]"
	 * By default, the topology is run locally under the name "ProcessLogTopo".
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String topologyName = "ProcessLogTopo";
		if(args.length >= 1){
			topologyName = args[0];
		}
		
		boolean runLocally = true;
		//集群模式
		if(args.length >= 2 && args[1].equalsIgnoreCase("remote")){
			runLocally = false;
		}
		
		LOG.info("Topology name: " + topologyName);
		ProcessLogTopology plt = new ProcessLogTopology(topologyName);
		if(runLocally){
			LOG.info("Running in local mode"); //本地模式
			plt.runLocally();
			System.err.println("aa");
		}
		else{
		     LOG.info("Running in remote (cluster) mode");
		     plt.runRemotely();
		}
	}
}
