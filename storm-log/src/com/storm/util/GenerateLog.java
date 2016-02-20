package com.storm.util;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;


public class GenerateLog {

	private static final Random random = new Random();
	private FileWriter writer = null;
	private static final String[] ips = new String[]{
		"192.168.1.1",
		"192.168.1.2",
		"192.168.1.3",
		"192.168.1.4",
		"192.168.1.5",
		};
	private static final String[] servlets = new String[]{
		//"/servlet/com.icbc.inbs.servlet.ICBCINBSEstablishSessionServlet",
		//"/servlet/AsynGetDataServlet",
		"a/servlet",
		"b/servlet",
		"c/servlet",
		"d/servlet"
	};
	
	private static final String[] https = new String[]{
		"http://1.html",
		"http://2.html",
		"http://3.html",
		"http://4.html",
		"http://5.html",
		"http://6.html",
		"http://7.html",
		"http://8.html",
		"http://9.html",
		"http://10.html"
	};
	
	private static final String[] returnCode = new String[]{
		"304","303","200","500","404","403"	};
	private static final String serverip = "10.1.1.1";
	//2015-08-23	11:00:00	pdccbeb.site1	10.1.1.1	GET	/icbc/new/servlet1	-	-	20.23.23.3	http://absfpasswd.html	Mozilla	304	304	-	0
    String output ;
	public String getOutput() {
		return output;
	}
	public void setOutput(String output) {
		this.output = output;
	}
	
	public GenerateLog(){
		try {
			writer  = new FileWriter("log//generatelog.txt",true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String Generate(){
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		String currentTime = df.format(new Date());// new Date()为获取当前系统时间
		String returnCodeStr = returnCode[random.nextInt(returnCode.length)];
		output = currentTime.split(" ")[0] + "\t"+
				currentTime.split(" ")[1] +  "\t" + 
				"pdccbeb.site1" + "\t" +
				serverip + "\t" + 
				"GET" + "\t" +
				servlets[random.nextInt(servlets.length)] + "\t" +
				"-" + "\t" +
				"-" + "\t" +
				ips[random.nextInt(ips.length)] + "\t" + 
				https[random.nextInt(https.length)] +  "\t" +
				"Mozilla" + "\t" +
				returnCodeStr + "\t" +
				returnCodeStr + "\t" +
				"-" + "\t" +
				"0";
				
		return output;
	}
	public void Write(String s){
		try {
//			Open();
			writer.write(s + "\n");
//			writer.write();
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		finally
//		{
//			Close();
//		}
	}
	
	public void Open()
	{
		try {
			writer  = new FileWriter("log//generatelog.txt",true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void Close()
	{
		if(writer != null){
			try {
				writer.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	protected void finalize()
	{
		System.err.println(GenerateLog.class + " finalize");
		if(writer != null){
			try {
				writer.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	public static void main(String args[])
	{
		GenerateLog glog = new GenerateLog();
		String srtLog = "";
		for(int i = 1; i <100 ; i ++){
			srtLog = glog.Generate();
			System.out.println(srtLog);
			glog.Write(srtLog);
			try {
				long wait = (long)(Math.random()* 2) + 1;
//				System.err.println(wait);
				Thread.sleep( wait * 500 );
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
}
