package com.dong.spark;

import java.io.File;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 排序的WordCount
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class SortWordCount {
	
	public static void main(String[] args) {
		SparkUtil.compute("SortWordCount", SortWordCount::sortWordCount);
	}
	
	public static void sortWordCount(JavaSparkContext sc) {
		String filePath = new File(".","/data/README.md").getAbsolutePath();
		JavaRDD<String> lines = sc.textFile(filePath);
		
		//按照单词出现的次数，降序排序
		lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
			 .mapToPair(word -> new Tuple2<>(word, 1))
			 .reduceByKey((x,y) -> x + y)
			 //交换Pair对的key,value
			 .mapToPair(v -> new Tuple2<>(v._2, v._1))
			 //降序排序
			 .sortByKey(false)
			 //再次交换Pair对的key,value
			 .mapToPair(v -> new Tuple2<>(v._2, v._1))
			 .foreach(tp -> System.out.println("单词:" + tp._1 + "，个数:" + tp._2));
			 
	}
}
