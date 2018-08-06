package com.dong.spark.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * TopN
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class TopN<E extends Comparable<E>> {
	
	private PriorityQueue<E> queue;
	private int maxSize;
	
	public TopN(int maxSize) {
		if(maxSize <= 0) {
			throw new IllegalArgumentException("maxSize不能小于等于0");
		}
		this.maxSize = maxSize;
		this.queue = new PriorityQueue<>(maxSize, Comparable::compareTo);
	}
	
	public void add(E e) {
		if(queue.size() < maxSize) {
			queue.add(e);
		} else {
			E peek = queue.peek();
			if(e.compareTo(peek) > 0) {
				queue.poll();
				queue.add(e);
			}
		}
	}
	
	public List<E> sortedList(){
		List<E> list = new ArrayList<>(queue);
		Collections.sort(list,(o1,o2) -> o2.compareTo(o1));
		return list;
	}
	
	public static void main(String[] args) {
		  List<Integer> list = Arrays.asList(1,3,8,5,6,7,4,2);
		  
		  TopN<Integer> top3 = new TopN<>(3);
		  for(Integer num : list) {
			  top3.add(num);
		  }
		  System.out.println(top3.sortedList());
	}
}
