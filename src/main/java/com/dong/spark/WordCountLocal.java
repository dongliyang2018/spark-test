package com.dong.spark;

import java.io.File;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * WordCount示例本地执行
 * @version 1.0 2018/08/05
 * @author dongliyang
 */
public class WordCountLocal {
	
	public static void main(String[] args) {
		
		/*
		 * 创建SparkConf对象，设置Spark应用的配置信息。
		 * 使用setMaster设置Spark应用程序要连接的Spark集群的master节点的url。
		 * 如果设置为local，则代表在本地运行
		 */
		SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = new File(".","/data/README.md").getAbsolutePath();
		JavaRDD<String> lines = sc.textFile(filePath);
		
		JavaPairRDD<String, Integer> wordCounts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
													   .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
													   .reduceByKey((x,y) -> x + y);
		
		wordCounts.foreach(tp -> System.out.println("Word: " + tp._1 + ", Count: " + tp._2));
		sc.close();
	}
}
