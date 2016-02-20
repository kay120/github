package com.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.util.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SpliterBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(SpliterBolt.class);
	String date = "";
	String time = "";
	String serverip = "";
	String url = "";
	String returnCode = "";
	String accessip ="";
	String[] tmp = null;
	static int num = 1;	

	public void execute(Tuple input, BasicOutputCollector collector) {
		logger.debug("SpliterBolt start");
		String line = input.getStringByField("msg");
		tmp = line.split("\\t");
		if(tmp.length < 15 ) return;
		date = tmp[0];
		time = tmp[1];
		serverip = tmp[3];		
		accessip = tmp[8];
		url = tmp[9];
		returnCode = tmp[11];
		
		collector.emit(new Values(date,time,accessip + "_" + serverip ,url,returnCode));
		logger.info("=======================  SpliterBolt  print============================================");
		logger.info( num++ + "--->" +accessip + "_" + serverip+" "+date + " " + time + ", "+ returnCode + ", " + url);
		logger.info("SpliterBolt end");
		logger.info("=======================  SpliterBolt  print end =======================================");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date","time","accessip_serverip","url","returnCode"));		
	}
}
