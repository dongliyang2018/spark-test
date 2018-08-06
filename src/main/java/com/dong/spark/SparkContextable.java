package com.dong.spark;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * SparkContextable
 * @version 1.0 2018/08/06
 * @author dongliyang
 */
public interface SparkContextable {
	
	void apply(JavaSparkContext sc);
}
