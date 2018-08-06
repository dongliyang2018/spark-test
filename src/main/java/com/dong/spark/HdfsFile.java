package com.dong.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 使用HDFS文件创建RDD
 * @version 1.0 2018/08/05
 * @author dongliyang
 */
public class HdfsFile {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("HdfsFile");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = "hdfs://localhost:9000/user/root/README.md";
		JavaRDD<String> lines = sc.textFile(filePath);
		
		//统计文件总字数，包含空格
		int wordCounts = lines.map(line -> line.length()).reduce((x,y) -> x + y);
		//统计文件总字数，去掉空格
//		int wordCounts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(word -> word.length()).reduce((x,y) -> x + y);
		sc.close();
		
		System.out.println("总字符数:" + wordCounts);
		
	}
}
