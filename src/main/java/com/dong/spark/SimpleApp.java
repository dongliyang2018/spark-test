package com.dong.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Spark基础示例
 * @version 1.0 2018/08/04
 * @author dongliyang
 */
public class SimpleApp {
	
	public static void main(String[] args) {
		
		String logFile = "/app/spark/README.md";
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();
		
		long numAs = logData.filter(line -> line.contains("a")).count();
		long numBs = logData.filter(line -> line.contains("b")).count();
		
		sc.close();
		System.out.println("Lines with a:" + numAs + ", lines with b:" + numBs);
	}
}
