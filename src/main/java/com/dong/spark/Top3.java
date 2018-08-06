package com.dong.spark;

import java.io.File;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 取最大的前3个数字
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class Top3 {
	public static void main(String[] args) {
		SparkUtil.compute("Top3", Top3::top3);
	}
	
	public static void top3(JavaSparkContext sc) {
		
		String filePath = new File(".","/data/top.txt").getAbsolutePath();
		JavaRDD<String> lines = sc.textFile(filePath);
		
		lines.mapToPair(line -> new Tuple2<>(Integer.parseInt(line), line))
			 .sortByKey(false)
			 .map(tp -> tp._1)
			 .take(3)
			 .forEach(x -> System.out.println(x));
		
	}
}
