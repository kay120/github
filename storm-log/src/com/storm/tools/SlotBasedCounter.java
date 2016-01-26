/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.storm.tools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.storm.util.LogValue;

/**
 * This class provides per-slot counts of the occurrences of objects.
 * <p/>
 * It can be used, for instance, as a building block for implementing sliding window counting of objects.
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlotBasedCounter implements Serializable {

  private static final long serialVersionUID = 1;

  private final List<HashMap<String, List<LogValue>>> listMapLogValue;
  private final int numSlots;

  public SlotBasedCounter(int numSlots) {
    if (numSlots <= 0) {
      throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
    }
    this.numSlots = numSlots;
    listMapLogValue = new ArrayList<HashMap<String,List<LogValue>>>(this.numSlots);
  }

  //  为某项的第slot个时隙添加 IP的LogValue
  //  当 list size() >= numSlots 需要remove 0
  public void incrementLogValue(String strIP, int curSlot,LogValue logValue) {	
	//如果listMapLogValue[curSlot] 没有map
	HashMap<String,List<LogValue>> ipMap;
	// listMapLogValue[curSlot]没有map时
	if(listMapLogValue.size() <= curSlot){
		ipMap = new HashMap<String,List<LogValue>>();
		listMapLogValue.add(ipMap);
	}else{
		ipMap = listMapLogValue.get(curSlot);
	}
		
	if(ipMap == null)
    {
    	ipMap = new HashMap<String,List<LogValue>>();
    }
    List<LogValue> ipValue = ipMap.get(strIP);
    if(ipValue == null){
    	ipValue = new ArrayList<LogValue>();
    }
    ipValue.add(logValue);
    ipMap.put(strIP, ipValue);
    System.err.println("listMapLogValue.size() :" + listMapLogValue.size()+ " curSlot :" + curSlot );
    listMapLogValue.set(curSlot, ipMap);
    System.err.println("listMapLogValue.size() :" + listMapLogValue.size()+ " curSlot :" + curSlot );
  } 

  public void printListMapListLogValue()
  {
	  System.err.println("输出ListMapIPListLogValue============================"+ listMapLogValue.size() + " numSlot:" + this.numSlots);
	  for(int i=0; i < listMapLogValue.size(); i++){
		  System.err.println("Slot num : " + i);
		  HashMap<String,List<LogValue>> ipMap = listMapLogValue.get(i);
		  for(String key : ipMap.keySet()){
			  System.err.println("===>IP: " + key);
			  List<LogValue> ipValue = ipMap.get(key);
			  for(int j =0 ;j < ipValue.size() ; j++){
				  System.err.println("=========> " + ipValue.get(j).getTime() + " " + ipValue.get(j).getServlet());
			  }
		  }
	  }
	  System.err.println("print over=========================================");
  }
  public void PopList(){
	  if(listMapLogValue.size() >= this.numSlots){
		  listMapLogValue.remove(0);
	  }
  }
  // 获取某项在某个slot的IP  Map
//  public long getCount(T obj, int slot) {
//    long[] counts = objToCounts.get(obj);
//    if (counts == null) {
//      return 0;
//    }
//    else {
//      return counts[slot];
//    }
//  }

   public HashMap<String, List<LogValue>> GetIPMaps() {
	   HashMap<String, List<LogValue>> result = new HashMap<String, List<LogValue>>();
	   
	   // 返回所有IP的 list副本
	   for(int i =0 ;i < listMapLogValue.size();i++){
		   HashMap<String,List<LogValue>> ipMap = listMapLogValue.get(i);
			  for(String key : ipMap.keySet()){
//				  System.err.println("===>IP: " + key);
				  List<LogValue> ipValue = ipMap.get(key);
				  List<LogValue> resultList = result.get(key);
				  if(resultList == null){
					  resultList = new ArrayList<LogValue>();
				  }
				  for(int j =0 ;j < ipValue.size() ; j++){
//					  System.err.println("=========>ipValue.size():"+ ipValue.size() + " " + ipValue.get(j).getTime() + " " + ipValue.get(j).getServlet());
					  resultList.add(new LogValue(ipValue.get(j).getTime(),ipValue.get(j).getServlet()));					  
				  }
				  result.put(key, resultList);
			  }
	   }
	   return result;
   }
  // 获取所有项在所有slot值的和
//  public Map<T, Long> getCounts() {
//    Map<T, Long> result = new HashMap<T, Long>();
//    for (T obj : objToCounts.keySet()) {
//      result.put(obj, computeTotalCount(obj));
//    }
//    return result;
//  }

//  private long computeTotalCount(T obj) {
//    long[] curr = objToCounts.get(obj);
//    long total = 0;
//    for (long l : curr) {
//      total += l;
//    }
//    return total;
//  }

  /**
   * Reset the slot count of any tracked objects to zero for the given slot.
   * // 把所有项的第slot个时隙置0
   * @param slot
   */
//  public void wipeSlot(int slot) {
//    for (T obj : objToCounts.keySet()) {
//      resetSlotCountToZero(obj, slot);
//    }
//  }

//  private void resetSlotCountToZero(T obj, int slot) {
//    long[] counts = objToCounts.get(obj);
//    counts[slot] = 0;
//  }

//  private boolean shouldBeRemovedFromCounter(T obj) {
//    return computeTotalCount(obj) == 0;
//  }

  /**
   * Remove any object from the counter whose total count is zero (to free up memory).
   */ // 把totalCount为0的项删掉
//  public void wipeZeros() {
//    Set<T> objToBeRemoved = new HashSet<T>();
//    for (T obj : objToCounts.keySet()) {
//      if (shouldBeRemovedFromCounter(obj)) {
//        objToBeRemoved.add(obj);
//      }
//    }
//    for (T obj : objToBeRemoved) {
//      objToCounts.remove(obj);
//    }
//  }

}
