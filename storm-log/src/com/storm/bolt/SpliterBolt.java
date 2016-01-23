package com.storm.bolt;

import java.util.HashMap;
import java.util.Map;

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

	String date = "";
	String time = "";
	String serverip = "";
	String servlet = "";
	String accessip ="";
	String[] tmp = null;
	static int num = 1;
	String p1 ="/servlet/com.icbc.inbs.servlet.ICBCINBSEstablishSessionServlet";
	
	String p2 = "/servlet/AsynGetDataServlet";
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(TupleHelpers.isTickTuple(input)){
			System.err.println("SpliterBolt 定时");
//			LOG.debug("Received tick tuple, triggering emit of current window counts");
		}else{
			System.err.println("SpliterBolt start");
			String line = input.getStringByField("msg");
//			System.err.println("SpliterBolt [" + System.currentTimeMillis() + "] line:" + line );
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
				System.err.println("splitBolt [time " + + System.currentTimeMillis() + "]: " + num++ + "--->" +accessip + "_" + serverip+"_"+date + " " + time + ", "+ servlet );
			}	
			System.err.println("SpliterBolt end");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date","time","accessip_serverip","servlet"));		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
	   Map<String, Object> conf = new HashMap<String, Object>();
	   conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
	   return conf;
	}

}
