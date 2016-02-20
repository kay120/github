package com.storm.util;
import com.storm.util.DateFmt;

public class IPCount {
	private String beginTime;
	private String endTime;
	private StringBuilder url;

	private int ipCounts;
	private int returnCodeCounts;
	
	public IPCount(){
		url = new StringBuilder();
		ipCounts = 0;
		returnCodeCounts = 0;
	}
	public IPCount(String beginTime, String endTime){
		this.beginTime = beginTime;
		this.endTime = endTime;
		ipCounts = 0;
		returnCodeCounts = 0;
		url = new StringBuilder();
	}
	
	public String AddUrl(String strUrl){
		url.append(strUrl + ";");
		return url.toString();
	}
	public String getUrl() {
		return url.toString();
	}
	
	public String getBeginTime() {
		return beginTime;
	}
	public void setBeginTime(String beginTime) {
		if(BeforeBeginTime(beginTime)){
			this.beginTime = beginTime;
		}
	}	
	
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		if(AfterEndTime(endTime)){
			this.endTime = endTime;
		}
	}
	public int getIpCounts() {
		return ipCounts;
	}
	public void setIpCounts(int ipCounts) {
		this.ipCounts = ipCounts;
	}
	public int getReturnCodeCounts() {
		return returnCodeCounts;
	}
	public void setReturnCodeCounts(int returnCodeCounts) {
		this.returnCodeCounts = returnCodeCounts;
	}
	
	public boolean BeforeBeginTime(String time)
	{
		try {
			if (DateFmt.parseDate(time).before(DateFmt.parseDate(this.beginTime))){
				return true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
//			return false;
		}
		return false;
	}
	
	public boolean AfterEndTime(String time){
		try {
			if(DateFmt.parseDate(time).after(DateFmt.parseDate(this.endTime))){
				return true;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public void SetValue(String time, int ipCounts, int returnCodeCounts){
		setIpCounts(ipCounts);
		setReturnCodeCounts(returnCodeCounts);
		setBeginTime(time);
		setEndTime(time);
	}
	public void Copy(IPCount ipCount) {
		this.beginTime = ipCount.getBeginTime();
		this.endTime = ipCount.getEndTime();
		setIpCounts(ipCount.getIpCounts());
		setReturnCodeCounts(ipCount.getReturnCodeCounts());
		url = new StringBuilder(ipCount.getUrl());
	}
}
