package com.storm.bolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WriterBolt extends BaseBasicBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(WriterBolt.class);
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String IPinfo = input.getStringByField("suspiciousIPInfo");
		logger.info(IPinfo);		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
