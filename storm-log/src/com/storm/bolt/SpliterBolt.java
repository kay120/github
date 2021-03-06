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
	String servlet = "";
	String accessip ="";
	String[] tmp = null;
	static int num = 1;
	String p1 ="/servlet/com.icbc.inbs.servlet.ICBCINBSEstablishSessionServlet";
	
	String p2 = "/servlet/AsynGetDataServlet";
	private long beginTime = System.currentTimeMillis();
	private long endTime = 0L;
	private boolean firstTime = true;
	public void execute(Tuple input, BasicOutputCollector collector) {
		// split 不定时
		if(TupleHelpers.isTickTuple(input)){
			
			endTime = System.currentTimeMillis();
			long diffTime = endTime - beginTime;
			logger.debug("SpliterBolt 定时: " + diffTime + " = " + diffTime/1000);
			firstTime = true;
//			LOG.debug("Received tick tuple, triggering emit of current window counts");
		}else{
			if(firstTime)
			{
				beginTime = System.currentTimeMillis();
				firstTime = false;
			}
			logger.debug("SpliterBolt start");
			String line = input.getStringByField("msg");
			date = "";
			time = "";
			serverip = "";
			servlet = "";
			accessip ="";
			tmp = line.split("\\t");
			if(tmp.length < 15 ) return;
			date = tmp[0];
			time = tmp[1];
			serverip = tmp[3];		
			servlet = tmp[5];		
			accessip = tmp[8];
			
			boolean bp1 = servlet.matches("(.*)" + p1 + "(.*)");
			boolean bp2 = servlet.matches("(.*)" + p2 + "(.*)");
			if( bp1 || bp2)
			{
				collector.emit(new Values(date,time,accessip + "_" + serverip ,servlet));
				logger.debug("splitBolt [time " + + System.currentTimeMillis() + "]: " + num++ + "--->" +accessip + "_" + serverip+"_"+date + " " + time + ", "+ servlet);
			}	
			logger.debug("SpliterBolt end");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date","time","accessip_serverip","servlet"));		
	}
}
