package com.dong.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 并行化集合创建RDD
 * @version 1.0 2018/08/05
 * @author dongliyang
 */
public class ParallelizeCollection {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("ParallelizeCollection")
										.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		//计算累加和
		int sum = numberRDD.reduce((x,y) -> x + y);
	
		sc.close();
		
		System.out.println("sum: " + sum);
	}
}
