package com.kay.bolt;

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
	int num =0;
	String p1 ="/servlet/com.icbc.inbs.servlet.ICBCINBSEstablishSessionServlet";
	
	String p2 = "/servlet/AsynGetDataServlet";
	
	public void execute(Tuple input, BasicOutputCollector collector) {
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
			System.err.println("splitBolt:" + num++ + "--->" +accessip + "_" + serverip+"_"+date + " " + time + ", "+ servlet );
		}		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	

}
