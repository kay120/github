package com.storm.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.tools.NthLastModifiedTimeTracker;

import com.storm.tools.SlidingWindowCounter;

import com.storm.util.IPCount;
import com.storm.util.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RollingCountBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(RollingCountBolt.class);
	private static final int NUM_WINDOW_CHUNKS = 5;
	private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
	private final int windowLengthInSeconds;
	private final int emitFrequencyInSeconds;
	private IPCount ipCount ;
	Map<String, IPCount> countsMap = null;
	private int num = 1;
	private NthLastModifiedTimeTracker lastModifiedTracker;
	private String matchCode = "403|404";
	//默认 emit 1分钟 时间窗5分钟
	
	public RollingCountBolt() {
		    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
		  }
	
	public RollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
	    this.windowLengthInSeconds = windowLengthInSeconds;
	    this.emitFrequencyInSeconds = emitFrequencyInSeconds;		
	}
	
	private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
	    return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
	  }

	String date = "";
	String time = "";
	String accessip_serverip = "";
	String url = "";
	String returnCode = "";
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {		
		countsMap = new HashMap<String, IPCount>();//		
		lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
		        this.emitFrequencyInSeconds));
	}
	
	int ipcount = 0;
	int returncodecount = 0;
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(TupleHelpers.isTickTuple(input)){
			logger.info("定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时: ");
			int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
		    lastModifiedTracker.markAsModified();
//		    logger.info("===========================before copy======================");
//		    printlnIPMapListValue(countsMap);
			emitCurrentWindowCounts(collector);
		}else{
			logger.info("==========================RollingCountBolt start===========================");
			date = input.getStringByField("date");
			time = input.getStringByField("time");
			accessip_serverip = input.getStringByField("accessip_serverip");
			url = input.getStringByField("url");
			returnCode = input.getStringByField("returnCode").trim();			
			
			logger.info(date + " " + 
						time + " " +
						accessip_serverip + " " + 
						url + " " + 
						returnCode);
			
			if(countsMap.containsKey(accessip_serverip))
			{		
				ipCount = countsMap.get(accessip_serverip);
				ipcount = ipCount.getIpCounts();
				returncodecount = ipCount.getReturnCodeCounts();
				logger.info("====contains================================");
				logger.info("contains : " + ipcount + " " + returncodecount);
			}else{
				ipCount = new IPCount( date + " " + time , date + " " + time);
				ipcount = 0;
				returncodecount = 0;
			}
			ipcount ++;
			
			if(returnCode.matches(matchCode)){
				returncodecount++ ;
				ipCount.AddUrl(url);
			}
			
			ipCount.SetValue(date + " " + time , ipcount , returncodecount);
			countsMap.put(accessip_serverip , ipCount);
			logger.info("=======================  RollingCountBolt  print============================================");
			logger.info( num++ + "----------->" +
						 accessip_serverip +
						 " "+date + " " + time + 
						 ", " + returnCode + 
						 ", " + url + 
						 ", ipcount :" + ipcount +
						 ", returncodecount" + returncodecount);
			
			logger.info("ipCount info :" + accessip_serverip  +
						 " begin:" + ipCount.getBeginTime() + 
						 " end:" + ipCount.getEndTime() + 
						 " ipcount:" + ipCount.getIpCounts() +
						 " returncodecount:" + ipCount.getReturnCodeCounts() +
						 " url:" + ipCount.getUrl());
			logger.info("RollingCountBolt end");
			logger.info("=======================  RollingCountBolt print end========================================");
		}
	}
		
	private void emitCurrentWindowCounts(BasicOutputCollector collector) {
		
		Map<String, IPCount> countsMapCopy = new HashMap<String, IPCount>();
		
		for(String ip: countsMap.keySet()){
			IPCount ipCountcopy = new IPCount();
			ipCountcopy.Copy(countsMap.get(ip));
			countsMapCopy.put(ip, ipCountcopy);
		}
		collector.emit(new Values(countsMapCopy));
		countsMap.clear();
//		logger.info("=========================countsMapCopy===================");
//		printlnIPMapListValue(countsMapCopy);
//		logger.info("=========================countsMap===================");
//		printlnIPMapListValue(countsMap);
	  }
	  
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("IPcountsMap"));
	}
	
	public void printlnIPMapListValue(Map<String, IPCount> result){
		for(String ip: result.keySet()){
			logger.info("==========>" + ip);
			logger.info("#####  beginTime:" + result.get(ip).getBeginTime() + "          endTime:" + result.get(ip).getEndTime());
			logger.info("#####   ipCounts:" + result.get(ip).getIpCounts() +  " returnCodeCounts:" + result.get(ip).getReturnCodeCounts());
			logger.info("#####  403/4 url:" + result.get(ip).getUrl());
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
	   Map<String, Object> conf = new HashMap<String, Object>();
	   conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
	   return conf;
	}
}
