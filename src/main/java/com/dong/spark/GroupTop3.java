package com.dong.spark;

import java.io.File;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 分组取Top3
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class GroupTop3 {
	
	public static void main(String[] args) {
		SparkUtil.compute("GroupTop3", GroupTop3::groupTop3);
	}
	
	public static void groupTop3(JavaSparkContext sc) {
		
		String filePath = new File(".","/data/score.txt").getAbsolutePath();
		JavaRDD<String> lines = sc.textFile(filePath);
		
		lines.mapToPair(line -> {
			String[] lineArr = line.split(" ");
			return new Tuple2<>(lineArr[0],Integer.parseInt(lineArr[1]));
		}).groupByKey()
		  .mapToPair(tp -> {
			  Integer[] top3 = new Integer[3];
			  for(Integer score : tp._2) {
				  for(int i = 0; i < 3; i++) {
					  if(top3[i] == null) {
						  top3[i] = score;
						  break;
					  } else if(score > top3[i]) {
						  for(int j = 2; j > i; j--) {
							  top3[j] = top3[j - 1];
						  }
						  top3[i] = score;
						  break;
					  }
				  }
			  }
			  
			  return new Tuple2<>(tp._1,Arrays.asList(top3));

		  }).foreach(x -> {
			  System.out.println("key:" + x._1 + ",value:" + x._2);
		  });;
		
	}
}
