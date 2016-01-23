package com.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.storm.util.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RollingCountBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);
	private static final int NUM_WINDOW_CHUNKS = 5;
	private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
	private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
	private final int windowLengthInSeconds;
	private final int emitFrequencyInSeconds;
	Map<String, Integer> countsMap = null;
	private int num = 1;
	
	public RollingCountBolt() {
		    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
		  }
	
	public RollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
	    this.windowLengthInSeconds = windowLengthInSeconds;
	    this.emitFrequencyInSeconds = emitFrequencyInSeconds;		    
	}

	String date = "";
	String time = "";
	String accessip_serverip = "";
	String servlet = "";
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {		
		countsMap = new HashMap<String, Integer>();//		
	}
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(TupleHelpers.isTickTuple(input)){
			System.err.println("RollingCountBolt 定时");
//			LOG.debug("Received tick tuple, triggering emit of current window counts");
		}else{
			System.err.println("RollingCountBolt start");
			date = input.getStringByField("date");
			time = input.getStringByField("time");
			accessip_serverip = input.getStringByField("accessip_serverip");
			servlet = input.getStringByField("servlet");
	
			//1 second access many times
			int count;
			
			if(countsMap.containsKey(accessip_serverip + "_" + date + " " + time))
			{
				count = countsMap.get(accessip_serverip + "_" + date + " " + time);
			}else{
				count = 0;
			}
			count ++;
			countsMap.put(accessip_serverip + "_" + date + " " + time, count);
			System.err.println("countBolt [time " + System.currentTimeMillis() + "]: " + num++ + "--->" +accessip_serverip+"_"+date + " " + time + ", "+ servlet + "=" + count);
			collector.emit(new Values(accessip_serverip, date, time, servlet ,count));
			System.err.println("RollingCountBolt end");
		}
	}
		
	
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
	   Map<String, Object> conf = new HashMap<String, Object>();
	   conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
	   return conf;
	}
}
