package com.dong.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Transformation操作示例
 * @version 1.0 2018/08/05
 * @author dongliyang
 */
public class TransformationOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TransformationOperation")
				.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		map(sc);
		filter(sc);
		flatMap(sc);
		groupByKey(sc);
		reduceByKey(sc);
		sortByKey(sc);
		join(sc);
		cogroup(sc);
		
		sc.close();
	}
	
	private static void map(JavaSparkContext sc) {
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		numberRDD.map(x -> x * 2).foreach(x -> System.out.println(x));
	}
	
	private static void filter(JavaSparkContext sc) {
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		numberRDD.filter(x -> x % 2 == 0).foreach(x -> System.out.println(x));
	}
	
	private static void flatMap(JavaSparkContext sc) {
		List<String> lines = Arrays.asList("line 1","line 2","line 3","line 4","line 5");
		JavaRDD<String> lineRDD = sc.parallelize(lines);
		
		lineRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
			   .foreach(word -> System.out.println(word));
	}
	
	private static void groupByKey(JavaSparkContext sc) {
		
		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65)
				);
		JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(scores);
		
		JavaPairRDD<String, Iterable<Integer>> groupScores = scoreRDD.groupByKey();
		groupScores.foreach(tp -> System.out.println("class: " + tp._1 + ", score: " + tp._2));
	}
	
	private static void reduceByKey(JavaSparkContext sc) {
		
		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65)
				);
		JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(scores);
		
		scoreRDD.reduceByKey((x,y) -> x + y)
		        .foreach(tp -> System.out.println("class: " + tp._1 + ", score: " + tp._2));
	}
	
	private static void sortByKey(JavaSparkContext sc) {
		
		List<Tuple2<Integer, String>> scores = Arrays.asList(
				new Tuple2<Integer, String>(80, "Zhang"),
				new Tuple2<Integer, String>(75, "Wang"),
				new Tuple2<Integer, String>(90, "Li"),
				new Tuple2<Integer, String>(65, "Zhao")
				);
		JavaPairRDD<Integer, String> scoreRDD = sc.parallelizePairs(scores).cache();
		
		scoreRDD.sortByKey()
		        .foreach(tp -> System.out.println("Score: " + tp._1 + ", Name: " + tp._2));
		
		scoreRDD.sortByKey(false)
        		.foreach(tp -> System.out.println("Score: " + tp._1 + ", Name: " + tp._2));
	}
	
	private static void join(JavaSparkContext sc) {
		
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "Zhang"),
				new Tuple2<Integer, String>(2, "Wang"),
				new Tuple2<Integer, String>(3, "Li")
				);
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(3, 60)
				);
		JavaPairRDD<Integer, String> studnetRDD = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
		
		studnetRDD.join(scoreRDD).sortByKey().foreach(tp -> {
			System.out.println("id: " + tp._1 + "，name: " + tp._2._1 + "，score: " + tp._2._2);
		});
	}
	
	private static void cogroup(JavaSparkContext sc) {
		
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "Zhang"),
				new Tuple2<Integer, String>(2, "Wang"),
				new Tuple2<Integer, String>(3, "Li")
				);
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(3, 60),
				new Tuple2<Integer, Integer>(1, 80),
				new Tuple2<Integer, Integer>(2, 70),
				new Tuple2<Integer, Integer>(3, 86)
				);
		JavaPairRDD<Integer, String> studnetRDD = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
		
		studnetRDD.cogroup(scoreRDD).sortByKey().foreach(tp -> {
			System.out.println("id: " + tp._1 + "，name: " + tp._2._1 + "，score: " + tp._2._2);
		});;
	}
}
