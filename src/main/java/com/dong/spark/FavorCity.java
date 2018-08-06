package com.dong.spark;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 统计每行出现的次数
 * @version 1.0 2018/08/05
 * @author dongliyang
 */
public class FavorCity {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FavorCity")
				.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath = new File(".","/data/citys.txt").getAbsolutePath();
		JavaRDD<String> lines = sc.textFile(filePath);
		
		lines.mapToPair(line -> new Tuple2<String,Integer>(line, 1))
			 .reduceByKey((x,y) -> x + y)
			 .sortByKey()
			 .foreach(tp -> System.out.println("城市:" + tp._1 + "，获得票数：" + tp._2));
		
		sc.close();
	}
}
