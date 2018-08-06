package com.dong.spark;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Action操作示例
 * @version 1.0 2018/08/05
 * @author dongliyang
 */
public class ActionOperation {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("ActionOperation").setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);
		reduce(sc);
		collect(sc);
		count(sc);
		take(sc);
//		saveAsTextFile(sc);
		countByKey(sc);
		sc.close();
	}
	
	private static void reduce(JavaSparkContext sc) {
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		int sum = numberRDD.reduce((x,y) -> x + y);
		System.out.println("sum: " + sum);
	}
	
	private static void collect(JavaSparkContext sc) {
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		List<Integer> doubleNumList = numberRDD.map(x -> x * 2).collect();
		doubleNumList.forEach(System.out::println);
	}
	
	private static void count(JavaSparkContext sc) {
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		long count = numberRDD.count();
		System.out.println("count: " + count);
	}
	
	private static void take(JavaSparkContext sc) {
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		List<Integer> top3Number = numberRDD.take(3);
		top3Number.forEach(System.out::println);
	}
	
//	private static void saveAsTextFile(JavaSparkContext sc) {
//		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
//		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
//		
//		numberRDD.saveAsTextFile("hdfs://localhost:9000/numbers");
//	}
	
	private static void countByKey(JavaSparkContext sc) {

		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65)
				);
		
		JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(scores);
		Map<String, Long> scoreMap = scoreRDD.countByKey();
		for(Map.Entry<String, Long> entry : scoreMap.entrySet()) {
			System.out.println("key: " + entry.getKey() + ", count: " + entry.getValue());
		}
	}
}
