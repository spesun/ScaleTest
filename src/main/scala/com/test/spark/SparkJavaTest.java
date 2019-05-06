package com.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by sun on 2016/10/28.
 */
public class SparkJavaTest {


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("MultiDataSource");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<String> list = new ArrayList<String>() ;
        list.add("11,22,33,44,55,11");
        list.add("aa,bb,cc,dd,ee,aa");
        JavaRDD<String>  listSpark = sc.parallelize(list);

        //map(listSpark);
        //flatmap(listSpark);
        groupByKey(listSpark);
        //reduceByKey(listSpark);


        sc.close();
    }

    public static JavaRDD<String[]> map(JavaRDD<String> listSpark) {
        Function<String, String[]> map = new Function<String,String[]>() {

            @Override
            public String[] call(String s) throws Exception {
                return s.split(",");
            }
        };

        JavaRDD<String[]> rdd = listSpark.map(map);

        rdd.foreach(new VoidFunction<String[]>() {
            @Override
            public void call(String[] strings) throws Exception {
                System.out.println(Arrays.asList(strings));
            }
        });

        return rdd;
    }

    public static  JavaRDD<String> flatmap(JavaRDD<String> listSpark) {
        FlatMapFunction<String, String> map = new FlatMapFunction<String,String>() {

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        };

        JavaRDD<String> rdd = listSpark.flatMap(map);

        rdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String strings) throws Exception {
                System.out.println(strings);
            }
        });

        return rdd;
    }

    public static void groupByKey(JavaRDD<String> listSpark) {

        listSpark = flatmap(listSpark);
        JavaPairRDD<String, Integer> pairs = listSpark.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s ,1);
            }
        });

        JavaPairRDD<String, Iterable<Integer>> counts = pairs.groupByKey();

        counts.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2.toString());
            }
        });
    }

    public static void reduceByKey(JavaRDD<String> listSpark) {
        listSpark = flatmap(listSpark);
       JavaPairRDD<String, Integer> pairs = listSpark.mapToPair(new PairFunction<String, String, Integer>() {

           @Override
           public Tuple2<String, Integer> call(String s) throws Exception {
               return new Tuple2<String, Integer>(s ,1);
           }
       });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });


       counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
           @Override
           public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
               System.out.println(stringIntegerTuple2._1() + "=" + stringIntegerTuple2._2());
           }
       });
    }


}
