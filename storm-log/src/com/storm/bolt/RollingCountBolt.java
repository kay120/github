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

import com.storm.util.LogValue;
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
	Map<String, Integer> countsMap = null;
	private int num = 1;
	private final SlidingWindowCounter ipListMap;
	  private NthLastModifiedTimeTracker lastModifiedTracker;
	//默认 emit 1分钟 时间窗5分钟
	private long beginTime = System.currentTimeMillis();
	private long endTime = 0L;
	private boolean firstTime = true;
	
	
	public RollingCountBolt() {
		    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
		  }
	
	public RollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
	    this.windowLengthInSeconds = windowLengthInSeconds;
	    this.emitFrequencyInSeconds = emitFrequencyInSeconds;		
	    ipListMap = new SlidingWindowCounter(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
	            this.emitFrequencyInSeconds));
	}
	
	private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
	    return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
	  }

	String date = "";
	String time = "";
	String accessip_serverip = "";
	String servlet = "";
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {	
		
		countsMap = new HashMap<String, Integer>();//		
		lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
		        this.emitFrequencyInSeconds));
	}
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(TupleHelpers.isTickTuple(input)){
			
			endTime = System.currentTimeMillis();
			long diffTime = endTime - beginTime;
			logger.debug("定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时: " + diffTime + " = " + diffTime/1000);
			firstTime = true;
			int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
		    lastModifiedTracker.markAsModified();
		    logger.debug("定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时定时: " + diffTime + " = " + diffTime/1000);
			emitCurrentWindowCounts(collector);
		}else{
			if(firstTime)
			{
				beginTime = System.currentTimeMillis();
				firstTime = false;
			}
			logger.debug("RollingCountBolt start");
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
			logger.debug("RollingcountBolt [time " + System.currentTimeMillis() + "]: " + num++ + "--->" +accessip_serverip+"_"+date + " " + time + ", "+ servlet + "=" + count);
			LogValue logValue = new LogValue();
			logValue.setTime(date + " " + time);
			logValue.setServlet(servlet);
			ipListMap.incrementCount(accessip_serverip, logValue);
			logger.debug("RollingCountBolt end");
		}
	}
		
	  private void emitCurrentWindowCounts(BasicOutputCollector collector) {
		  ipListMap.printlnListIPMapListValue();
		  
		  //获取所有的Map  IP list
		  HashMap<String, List<LogValue>> result = ipListMap.getCountsThenAdvanceWindow();
		  
		  ipListMap.printlnIPMapListValue(result);
		  collector.emit(new Values(result));
	  }
	  
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("IPMap"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
	   Map<String, Object> conf = new HashMap<String, Object>();
	   conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
	   return conf;
	}
}
