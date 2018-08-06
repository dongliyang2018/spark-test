package com.dong.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class BroadcastVariable {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("BroadcastVariable")
				.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		int factor = 3;
		//广播变量
		final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
	
		List<Integer> numberList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numberList);
		
		numberRDD.map(x -> x * factorBroadcast.getValue())
				 .foreach(x -> System.out.println(x));
	
		sc.close();
	}
}
