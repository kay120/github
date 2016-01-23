package com.kay.storm.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.kay.hbase.HBaseDAOImp;

import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.kay.storm.util.TupleHelpers;
public class CountBolt extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<String, Integer> countsMap = null;
	private int num = 1;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		countsMap = new HashMap<String, Integer>();
//		
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if(TupleHelpers.isTickTuple(input)){
			System.err.println("countbolt 定时");
		}else{
		String date = input.getStringByField("date");
		String time = input.getStringByField("time");
		String accessip_serverip = input.getStringByField("accessip_serverip");
		String servlet = input.getStringByField("servlet");
		
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
		System.err.println("countBolt:" + num++ + "--->" +accessip_serverip+"_"+date + " " + time + ", "+ servlet + "=" + count);
		collector.emit(new Values(accessip_serverip, date, time, servlet ,count));
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("accessip_serverip","date","time", "servlet" ,"count"));
	}

}
