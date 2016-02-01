package com.storm.bolt;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.util.DateFmt;
import com.storm.util.LogValue;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StatisticBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(StatisticBolt.class);
	private static final String S1 = "/servlet/com.icbc.inbs.servlet.ICBCINBSEstablishSessionServlet";
	private static final String S2 = "/servlet/AsynGetDataServlet";
	private static final long MILLIS_IN_SEC = 1000;
	private static final long SEC_IN_MIT = 60;
	//per minitue login times
	private static long thredholdLoginTimes = 12;
	private static double threadholdSuccessRate = 0.3;
	public void execute(Tuple input, BasicOutputCollector collector) {
		@SuppressWarnings("unchecked")
		HashMap<String, List<LogValue>> result = (HashMap<String, List<LogValue>>)input.getValue(0);
		//按时间排序
		for(String key:result.keySet()){
			Collections.sort(result.get(key));
		}
		logger.info("========================== print ================================================");
		printlnIPMapListValue(result);
		logger.info("============================ static ==============================================");
		String emitStrin = StatisticIP(result);
		collector.emit(new Values(emitStrin));
	}

	private String StatisticIP(HashMap<String, List<LogValue>> result) {
		//格式    IP_SERVERIP\firsttime\tendtime\t(yyyy-MM-dd HH:mm:ss);  IP以分号分隔，内容以\t分隔
		// 信息包括 IP_SERVERIP  firsttime  endtime  
		String outIPInfo = "";
		// 获取 每个 IP 访问集
		for(String key:result.keySet()){
			int C1 = 0 ;
			int C2 = 0 ;
			List<LogValue> list = result.get(key);		
			Date firstTime = list.get(0).getDateTime();
			Date endTime = list.get(list.size()-1).getDateTime();
			long diffTime = 0L;
			diffTime = (endTime.getTime()- firstTime.getTime()) /MILLIS_IN_SEC;
			logger.info("=====================================================");
			logger.info("===> IP : " + key);
			logger.info("frist time: " + firstTime +
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
			// 上取整，最小单位为分钟
			long elapsedMinute = (long) Math.ceil((diffTime * 1.0 )/ SEC_IN_MIT) ;
			elapsedMinute = elapsedMinute == 0 ? 1 : elapsedMinute;
			logger.info("C1 : {} C2 : {} elapsedMinute: {}" , C1 ,  C2 , elapsedMinute);
			
			if( C1 <= 0 ){
				logger.info(" C1  = 0  C1 = " + C1 + " elapsedMinute:" + elapsedMinute);
				continue;
			}else{
				if( (C1 / elapsedMinute) > thredholdLoginTimes && ( ((C2*1.0)/ C1 ) < threadholdSuccessRate)){
					logger.debug("C1 per minute :{} C2/C1: {}" ,(C1 / elapsedMinute),(C2*1.0)/ C1);
					outIPInfo = outIPInfo + ";" + key + "\t" + 
												firstTime + "\t" + 
												endTime + "\t" 											
												;
					
//					System.err.println(outIPInfo);
				}
			}
		}	
		logger.info("========================================================");
		logger.info("return outIPInfo" + outIPInfo);
		logger.info("========================================================");
//		logger.info("=====================================================");
		return outIPInfo;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("suspiciousIPInfo"));
	}

	public void printlnIPMapListValue(HashMap<String, List<LogValue>> result){
		for(String key : result.keySet()){
			logger.info("===>IP: " + key);
			List<LogValue> ipValue = result.get(key);
			for(int i =0 ;i < ipValue.size(); i ++){
				logger.info("=========> " + ipValue.get(i).getTime() + " " + ipValue.get(i).getServlet());
			}
		}
	}
}
