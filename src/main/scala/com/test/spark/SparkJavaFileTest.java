package com.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkJavaFileTest {

	
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> distFile = sc.textFile("hdfs://dcp2/tmp/testspark");


		distFile.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String str) throws Exception {
				System.out.println(str);
			}
		});
		
		
		distFile.saveAsTextFile("hdfs://dcp2/tmp/testspark_spark");
		System.out.println("aaaaa=" + distFile.collect());
		
		Thread.currentThread().sleep(100000);
		sc.stop();
		sc.close();
	}
}
