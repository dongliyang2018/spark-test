package com.dong.spark;

import java.io.File;
import java.time.Duration;
import java.time.Instant;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class Persist {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Persist")
				.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = new File(".","/data/README.md").getAbsolutePath();
		JavaRDD<String> lines = sc.textFile(filePath).cache();
		
		Instant start = Instant.now();
		long lineCount = lines.count();
		Instant end = Instant.now();
		
		System.out.println("line count:" + lineCount + ", cost: " + Duration.between(start, end).toMillis() + " millis");
		
		
		start = Instant.now();
		lineCount = lines.count();
		end = Instant.now();
		
		System.out.println("line count:" + lineCount + ", cost: " + Duration.between(start, end).toMillis() + " millis");
		
		sc.close();
	}
}
