package com.kay.storm.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.emitter.Emitable;

import com.kay.storm.util.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSpliter extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(TupleHelpers.isTickTuple(input)){
			System.err.println("spliterbolt 定时");
		}else{
			String line = input.getStringByField("msg");
			String date = "";
			String time = "";
			String serverip = "";
			String servlet = "";
			String accessip ="";
			String[] tmp = line.split("\\t");
			if(tmp.length < 15 ) return;
			date = tmp[0];
			time = tmp[1];
			serverip = tmp[3];		
			servlet = tmp[5];		
			accessip = tmp[8];
	
			String p1 ="/servlet/com.icbc.inbs.servlet.ICBCINBSEstablishSessionServlet";
			
			String p2 = "/servlet/AsynGetDataServlet";
			boolean bp1 = servlet.matches("(.*)" + p1 + "(.*)");
			boolean bp2 = servlet.matches("(.*)" + p2 + "(.*)");
			if( bp1 || bp2)
			{
				collector.emit(new Values(date,time,accessip + "_" + serverip ,servlet));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date","time","accessip_serverip","servlet"));

	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
	   Map<String, Object> conf = new HashMap<String, Object>();
	   conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5*1000);
	   return conf;
	}
}
