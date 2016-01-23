package com.kay.storm.topology;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.kay.storm.bolt.CountBolt;
import com.kay.storm.bolt.WordSpliter;
import com.kay.storm.bolt.WriterBolt;
import com.kay.storm.spout.MessageScheme;
//import com.kay.storm.spout.StormFileSpout;

public class KafkaTopo {

	public static void main(String[] args) throws Exception {
		
		String topic = "logkafka";
		String zkRoot = "/storm";
		String spoutId = "KafkaSpoutlog";
		BrokerHosts brokerHosts = new ZkHosts("master1:2181,master2:2181,slave1:2181"); 
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic , zkRoot, spoutId);
		spoutConfig.forceFromStart = true;
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		TopologyBuilder builder = new TopologyBuilder();
		//KafkaSpout
		spoutId = "accesslog";
		System.out.println("++++++++++++++++++++++++++++++++++++begin++++++++++++++++++++++++++++");
		builder.setSpout(spoutId, new KafkaSpout(spoutConfig));
		builder.setBolt("word-spilter", new WordSpliter(),4).shuffleGrouping(spoutId);
		builder.setBolt("word-count", new CountBolt(),4).fieldsGrouping("word-spilter", new Fields("accessip_serverip"));
		builder.setBolt("writer", new WriterBolt(),1).shuffleGrouping("word-count");
		Config conf = new Config();
		conf.setNumWorkers(4);
		conf.setNumAckers(0);
		conf.setDebug(true);
		
		if (args != null && args.length > 0) {
			
			conf.setNumWorkers(4);
			//锟结交topology锟斤拷storm锟斤拷群锟斤拷锟斤拷锟斤拷
			StormSubmitter.submitTopology("testtopo", conf,
					builder.createTopology());
			System.out.println("=========================================cluster========================");
		} else {
			conf.setMaxTaskParallelism(4);
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
			System.out.println("++++++++++++++++++++++++++localcluster++++++++++++++++++"+ args.length);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local-host", conf, builder.createTopology());
//
			System.out.println("localcluster");
//			Thread.sleep(10000);
//			cluster.shutdown();
//			System.out.println("++++++++++++++++++++++++++localcluster++++++++++++++++++"+ args.length);
		}
//		Utils.sleep(10000);
	}
}
