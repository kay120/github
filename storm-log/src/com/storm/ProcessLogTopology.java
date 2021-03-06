package com.storm;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.storm.ProcessLogTopology;
import com.storm.bolt.RollingCountBolt;
import com.storm.bolt.SpliterBolt;
import com.storm.bolt.StatisticBolt;
import com.storm.bolt.WriterBolt;
import com.storm.spout.MessageScheme;
import com.storm.spout.MySout;
import com.storm.util.StormRunner;

public class ProcessLogTopology {
	private static final Logger logger = LoggerFactory.getLogger(ProcessLogTopology.class);
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
		String statisticId = "statistic";
		String writerId = "writer";
		String topic = "logkafka";
		String zkRoot = "/storm";
		BrokerHosts brokerHosts = new ZkHosts("master1:2181,master2:2181,slave1:2181"); 
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic , zkRoot, spoutId);
		spoutConfig.forceFromStart = true;
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
//		builder.setSpout(spoutId, new KafkaSpout(spoutConfig));
		builder.setSpout(spoutId, new MySout(),20);
		builder.setBolt(spliterId, new SpliterBolt(),1).shuffleGrouping(spoutId);
		// 时间窗为2分钟， 半分钟发一次
		builder.setBolt(counterId, new RollingCountBolt(20,5)).fieldsGrouping(spliterId, new Fields("accessip_serverip"));
		builder.setBolt(statisticId, new StatisticBolt()).shuffleGrouping(counterId);	
		builder.setBolt(writerId, new WriterBolt()).shuffleGrouping(statisticId);
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}
	
	public void runLocally() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig);
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
		
		logger.info("Topology name: " + topologyName);
		ProcessLogTopology plt = new ProcessLogTopology(topologyName);
		if(runLocally){			
			plt.runLocally();
			logger.info("========================================");
			logger.info("Running in local mode"); //本地模式
			logger.info("========================================");
		}
		else{
			logger.info("Running in remote (cluster) mode");
		     plt.runRemotely();
		}
	}	
}
