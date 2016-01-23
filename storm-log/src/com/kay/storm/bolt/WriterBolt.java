package com.kay.storm.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.base.BaseDateTime;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.kay.hbase.*;
import com.kay.storm.util.TupleHelpers;
/**
 * 
 * 
 *
 */
public class WriterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	public static Configuration conf;
	int num = 1;	
	private Map stormConf; 
    private TopologyContext context;
	HBaseDAO hbasedao = null;
	List<Put> puts = null;
	private Map<String,String> servletMap = null;
	private Map<String,String> countMap = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		hbasedao = new HBaseDAOImp();
		puts = new ArrayList<Put>();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}	
	
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(TupleHelpers.isTickTuple(input)){
			System.err.println("writebolt 定时  puts.size():" + puts.size());
			hbasedao.save(puts, "log");
			puts.clear();
		}else{
			String date = input.getStringByField("date");
			String time = input.getStringByField("time");
			String accessip_serverip = input.getStringByField("accessip_serverip");
			String servlet = input.getStringByField("servlet");
			int count = input.getIntegerByField("count");
	
			Put put = new Put(Bytes.toBytes(accessip_serverip+"_"+date + " " + time));
			put.add(Bytes.toBytes("accesslog"),Bytes.toBytes("servlet"),Bytes.toBytes(servlet));
			put.add(Bytes.toBytes("accesslog"),Bytes.toBytes("count"),Bytes.toBytes(count ));
			puts.add(put);

			System.err.println("WriterBolt:" + num++ + "--->" +accessip_serverip+"_"+date + " " + time + ", "+ servlet + "=" + count);
	
			//System.err.println(input.getSourceComponent() + " equals " + Constants.SYSTEM_COMPONENT_ID);
			
	//		hbasedao.insert("log", accessip_serverip+"_"+date + " " + time, "accesslog", "servlet" , servlet);
	//		hbasedao.insert("log", accessip_serverip+"_"+date + " " + time, "accesslog", "count" , count + "");
		}
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
	   Map<String, Object> conf = new HashMap<String, Object>();
	   conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5*1000);
	   return conf;
	}
}
