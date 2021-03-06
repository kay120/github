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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.util.LogValue;

/**
 * This class counts objects in a sliding window fashion.
 * <p/>
 * It is designed 1) to give multiple "producer" threads write access to the counter, i.e. being able to increment
 * counts of objects, and 2) to give a single "consumer" thread (e.g. {@link PeriodicSlidingWindowCounter}) read access
 * to the counter. Whenever the consumer thread performs a read operation, this class will advance the head slot of the
 * sliding window counter. This means that the consumer thread indirectly controls where writes of the producer threads
 * will go to. Also, by itself this class will not advance the head slot.
 * <p/>
 * A note for analyzing data based on a sliding window count: During the initial <code>windowLengthInSlots</code>
 * iterations, this sliding window counter will always return object counts that are equal or greater than in the
 * previous iteration. This is the effect of the counter "loading up" at the very start of its existence. Conceptually,
 * this is the desired behavior.
 * <p/>
 * To give an example, using a counter with 5 slots which for the sake of this example represent 1 minute of time each:
 * <p/>
 * <pre>
 * {@code
 * Sliding window counts of an object X over time
 *
 * Minute (timeline):
 * 1    2   3   4   5   6   7   8
 *
 * Observed counts per minute:
 * 1    1   1   1   0   0   0   0
 *
 * Counts returned by counter:
 * 1    2   3   4   4   3   2   1
 * }
 * </pre>
 * <p/>
 * As you can see in this example, for the first <code>windowLengthInSlots</code> (here: the first five minutes) the
 * counter will always return counts equal or greater than in the previous iteration (1, 2, 3, 4, 4). This initial load
 * effect needs to be accounted for whenever you want to perform analyses such as trending topics; otherwise your
 * analysis algorithm might falsely identify the object to be trending as the counter seems to observe continuously
 * increasing counts. Also, note that during the initial load phase <em>every object</em> will exhibit increasing
 * counts.
 * <p/>
 * On a high-level, the counter exhibits the following behavior: If you asked the example counter after two minutes,
 * "how often did you count the object during the past five minutes?", then it should reply "I have counted it 2 times
 * in the past five minutes", implying that it can only account for the last two of those five minutes because the
 * counter was not running before that time.
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlidingWindowCounter implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(SlidingWindowCounter.class);
  private SlotBasedCounter objCounter;
  private int headSlot;
  private int tailSlot;
  private int windowLengthInSlots;

  public SlidingWindowCounter(int windowLengthInSlots) {
    if (windowLengthInSlots < 2) {
      throw new IllegalArgumentException(
          "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
    }
    this.windowLengthInSlots = windowLengthInSlots;
    this.objCounter = new SlotBasedCounter(this.windowLengthInSlots);

    this.headSlot = 0;
    this.tailSlot = slotAfter(headSlot);
  }

  public void incrementCount(String strIp, LogValue logValue) {
    objCounter.incrementLogValue(strIp ,headSlot, logValue);
  }

  /**
   * Return the current (total) counts of all tracked objects, then advance the window.
   * <p/>
   * Whenever this method is called, we consider the counts of the current sliding window to be available to and
   * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
   * objects within the next "chunk" of the sliding window.
   *
   * @return The current (total) counts of all tracked objects.
 * @throws Exception 
   */
  // 定时器到后 数组 headSlot 向前移动
  public HashMap<String,List<LogValue>> getCountsThenAdvanceWindow()  {
//    Map<T, Long> counts = objCounter.getCounts();// 获取所有key在各个slot的总数
//    objCounter.wipeZeros();// 删掉统计数为0的项
//    objCounter.wipeSlot(tailSlot);// 把tail指向的slot清0
    advanceHead();// head和tail后移
//    return counts;// return 一开始获取的slot总数    
    //当 list 满时 清楚 0
    HashMap<String, List<LogValue>> result = new HashMap<String, List<LogValue>>();
    result = objCounter.GetIPMaps();
    objCounter.PopList();
    return result;
  }

  public void printlnListIPMapListValue(){
	  objCounter.printListMapListLogValue();
  }
  
  public void printlnIPMapListValue(HashMap<String, List<LogValue>> result){
//	  System.err.println(this.getClass().getName());
	  for(String key : result.keySet()){
		  logger.info("================================================================================");
		  logger.info("===>IP: " + key);
		  List<LogValue> ipValue = result.get(key);
		  for(int i =0 ;i < ipValue.size(); i ++){
			  logger.info("=========> " + ipValue.get(i).getTime() + " " + ipValue.get(i).getServlet());
		  }
		  logger.info("================================================================================");
	  }
  }
  
  private void advanceHead() {
//    headSlot = tailSlot;
//    tailSlot = slotAfter(tailSlot);
	//如果list 没满 headSlot ++ 否则指向最后一个
    if(headSlot < windowLengthInSlots - 1)
    {
    	headSlot++;
    }else
    {
    	headSlot = windowLengthInSlots - 1;
    }
  }

  private int slotAfter(int slot) {
    return (slot + 1) % windowLengthInSlots;
  }

}
