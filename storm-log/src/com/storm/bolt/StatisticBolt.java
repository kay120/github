package com.storm.bolt;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.util.DateFmt;
import com.storm.util.IPCount;
import com.storm.util.LogValue;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StatisticBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(StatisticBolt.class);
	private static final long MILLIS_IN_SEC = 1000;
	private static final long SEC_IN_MIT = 60;
	Map<String, IPCount> countsMap = null;

	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
		countsMap = (Map<String, IPCount>)input.getValue(0);
		
		logger.info("==========================StatiBolt print ================================================");
		printlnIPMapListValue(countsMap);
		logger.info("==========================StatiBolt print end ============================================");
//		collector.emit(new Values());
	}

	private String StatisticIP(HashMap<String, List<LogValue>> result) {
		String outIPInfo = "";		
		logger.info("========================================================");
		logger.info("return outIPInfo:" + outIPInfo);
		logger.info("========================================================");
		return outIPInfo;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("suspiciousIPInfo"));
	}

	public void printlnIPMapListValue(Map<String, IPCount> result){
		for(String ip: result.keySet()){
			logger.info("==========>" + ip);
			logger.info("#####  beginTime:" + result.get(ip).getBeginTime() + "          endTime:" + result.get(ip).getEndTime());
			logger.info("#####   ipCounts:" + result.get(ip).getIpCounts() +  " returnCodeCounts:" + result.get(ip).getReturnCodeCounts());
			logger.info("#####  403/4 url:" + result.get(ip).getUrl());
		}
	}
}
