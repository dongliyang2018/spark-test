package com.dong.spark;

import java.io.File;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 二次排序，首先按照第一列排序，第一列相同，按照第二列排序
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class SecondarySort {
	
	public static void main(String[] args) {
		SparkUtil.compute("SecondarySort", SecondarySort::secondarySort);
	}
	
	public static void secondarySort(JavaSparkContext sc) {
		
		String filePath = new File(".","/data/sort.txt").getAbsolutePath();
		JavaRDD<String> lines = sc.textFile(filePath);
		
		lines.mapToPair(line -> {
			String[] lineArr = line.split(" ");
			SecondarySortKey key = new SecondarySortKey(Integer.parseInt(lineArr[0]),Integer.parseInt(lineArr[1]));
			return new Tuple2<>(key,line);
		}).sortByKey()
		.map(tp -> tp._2)
		.foreach(x -> System.out.println(x));;
		
	}
}
