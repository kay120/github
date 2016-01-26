package com.storm.bolt;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.storm.util.DateFmt;
import com.storm.util.LogValue;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class StatisticBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String S1 = "/servlet/com.icbc.inbs.servlet.ICBCINBSEstablishSessionServlet";
	private static final String S2 = "/servlet/AsynGetDataServlet";
	public void execute(Tuple input, BasicOutputCollector collector) {
		@SuppressWarnings("unchecked")
		HashMap<String, List<LogValue>> result = (HashMap<String, List<LogValue>>)input.getValue(0);
		//按时间排序
		for(String key:result.keySet()){
			Collections.sort(result.get(key));
		}
		printlnIPMapListValue(result);
		StatisticIP(result);
	}

	private void StatisticIP(HashMap<String, List<LogValue>> result) {
		
		for(String key:result.keySet()){
			int C1 = 0 ;
			int C2 = 0 ;
			List<LogValue> list = result.get(key);		
			Date firstTime = list.get(0).getDateTime();
			Date endTime = list.get(list.size()-1).getDateTime();
			long diffTime = 0L;
			diffTime = (endTime.getTime()- firstTime.getTime()) /1000;
			System.err.println("===> IP : " + key);
			System.err.println("frist time: " + firstTime +
							 "\n  end time: " + endTime  +
							 "\n  difftime: " + diffTime);
			for(int i =0 ; i < list.size(); i++){
				
				//先判断有多少次 S1
				if(list.get(i).getServlet().matches(S1)){
					C1++;
					//3秒内出现S2
					for(int j = i+1 ; j< list.size(); j++){
						long innerdiffTime = 0L;
						innerdiffTime = (list.get(j).getDateTime().getTime() - list.get(i).getDateTime().getTime())/1000;
						if( innerdiffTime >3)	break;
						if( list.get(j).getServlet().matches(S2)){
							C2++;
							break;
						}
					}					
				}
			}
			System.err.println("C1 : " + C1 + " " + C2);			
		}		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public void printlnIPMapListValue(HashMap<String, List<LogValue>> result){
		  System.err.println(this.getClass().getName());
		  
		  for(String key : result.keySet()){
			  System.err.println("===>IP: " + key);
			  List<LogValue> ipValue = result.get(key);
			  for(int i =0 ;i < ipValue.size(); i ++){
				  System.err.println("=========> " + ipValue.get(i).getTime() + " " + ipValue.get(i).getServlet());
			  }
		  }
	  }
	
	

}
