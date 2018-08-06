package com.dong.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * WordCount示例Spark环境执行
 * @version 1.0 2018/08/05
 * @author dongliyang
 */
public class WordCountCluster {
	
	public static void main(String[] args) {
		
		/*
		 * 创建SparkConf对象，设置Spark应用的配置信息。
		 * 使用setMaster设置Spark应用程序要连接的Spark集群的master节点的url。
		 * 如果设置为local，则代表在本地运行
		 */
		SparkConf conf = new SparkConf().setAppName("WordCountCluster");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "/app/spark/README.md";
		JavaRDD<String> lines = sc.textFile(filePath);
		
		JavaPairRDD<String, Integer> wordCounts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
													   .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
													   .reduceByKey((x,y) -> x + y);
		
		wordCounts.foreach(tp -> System.out.println("Word: " + tp._1 + ", Count: " + tp._2));
		sc.close();
		
		//打包上传到Spark环境，执行命令：spark-submit --class "com.dong.spark.WordCountCluster" --master local[4] spark-test-1.0.0.jar
	}
}
