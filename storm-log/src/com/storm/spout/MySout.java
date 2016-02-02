package com.storm.spout;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.util.GenerateLog;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySout extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(MySout.class);
	SpoutOutputCollector collector;
	private GenerateLog gLog = null;
	String strLog = null;
	private static int num = 0;
	@Override
	public void nextTuple() {
		strLog = gLog.Generate();
		num ++;
		logger.debug(num + "===>" + strLog);
		gLog.Write(strLog);
		collector.emit(new Values(strLog));
		try {
			long wait = (long)(Math.random()* 2) + 1;
//			System.err.println(wait);
			Thread.sleep( wait * 1000 );
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		gLog = new GenerateLog();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msg"));
		
	}

}
