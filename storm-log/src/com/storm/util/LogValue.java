package com.storm.util;
import java.util.Date;

import com.storm.util.DateFmt;
public class LogValue implements Comparable<LogValue>{
	String time;
	String servlet;

	public LogValue() {
	}
	
	public LogValue(String time, String servlet){
		this.time = time;
		this.servlet = servlet;
	}

	public String getTime() {
		return time;
	}

	public Date getDateTime(){
		try {
			return DateFmt.parseDate(time);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	public void setTime(String time) {
		this.time = time;
	}

	public String getServlet() {
		return servlet;
	}

	public void setServlet(String servlet) {
		this.servlet = servlet;
	}

	@Override
	public int compareTo(LogValue logValue) {
		Date d1 = null;
		try {
			d1 = DateFmt.parseDate(this.getTime());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Date d2 = null;
		try {
			d2 = DateFmt.parseDate(logValue.getTime());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if( d1 == null && d2 == null){
			return 0;
		}
		if( d1 == null)
			return -1;
		if( d2 == null)
			return 1;
		return d1.compareTo(d2);
	}
}
