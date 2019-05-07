package com.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamTest {
	
	
	
	
	
	public static void main(String[] args) {
	
		final String hdfsName = "dcpha";
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(600));
	    
	    System.out.println("yun nan:" + hdfsName);
	    JavaDStream<String> lines= ssc.textFileStream("hdfs://" + hdfsName + "/tmp/testspark/");    
	    
	   System.out.println(lines.count() + "aaaaaaaaaa");
	    lines.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {

			@Override
			public Void call(JavaRDD<String> rdd, Time time) throws Exception {
					System.out.println("bbbbbb=" + rdd.collect());
					rdd.saveAsTextFile("hdfs://" + hdfsName +"/tmp/testspark_output/" + time.toString() );
					return null;
			}
	    	
	    });
	    
	    //lines.count().print();
	    //lines.dstream().saveAsTextFiles("hdfs://dcp2/tmp/testspark111", "txt");
	    
	    lines.print();
	    ssc.start();
	    ssc.awaitTermination();
	    ssc.close();
	}

}
