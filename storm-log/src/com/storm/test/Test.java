package com.storm.test;

import static org.junit.Assert.*;

public class Test {
	@org.junit.Test
	public void testName() throws Exception {
		String matchCode = "404|403";
		String code = "405";
		System.err.println(code.matches(matchCode));		
	}
	
	@org.junit.Test
	public void test2() {
		String url = "acwe;sdfe;";
		String tmp[] = url.split(";");
		System.err.println("begin");
		for(int i = 0 ; i < tmp.length ; i++){
			System.err.println(tmp[i]);
		}
		System.err.println("end");
	}
	
	@org.junit.Test
	public void testStringBuilder(){
		String a = "a";
		String b = "b";
		StringBuilder c = new StringBuilder();
		System.err.println("before c is :" + c + "#");
		c.append(a + ";");
		c.append(a + ";");
		c.append(b + ";");
		
		System.err.println("append after c is :" + c + "#");
	}
}
