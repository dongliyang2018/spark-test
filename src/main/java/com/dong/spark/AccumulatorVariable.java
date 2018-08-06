package com.dong.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 累加变量
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
@SuppressWarnings("deprecation")
public class AccumulatorVariable {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Accumulator")
				.setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);
		Accumulator<Integer> sum = sc.accumulator(0);
		
		List<Integer> numberList = Arrays.asList(1,2,3,4,5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numberList);
		
		numberRDD.foreach(x -> sum.add(x));
		
		System.out.println("sum:" + sum.value());
		sc.close();
	}
}
