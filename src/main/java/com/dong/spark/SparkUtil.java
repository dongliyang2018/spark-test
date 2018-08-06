package com.dong.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Spark帮助类
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public class SparkUtil {
	
	public static void compute(String appName,SparkContextable sparkContextable) {
		compute(appName, "local", sparkContextable);
	}
	
	public static void compute(String appName,String master,SparkContextable sparkContextable) {
		SparkConf conf = new SparkConf().setAppName(appName);
		if(master != null && !master.trim().equals("")) {
			conf.setMaster(master);
		}
		JavaSparkContext sc = new JavaSparkContext(conf);
		sparkContextable.apply(sc);
		sc.close();
	}
}
