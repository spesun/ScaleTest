package com.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Properties;

public class GuijiMysqlJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("MultiDataSource");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlsc = new SQLContext(sc);
        String url="jdbc:mysql://kf-master-db:3306/kf-workorder?useUnicode=true&characterEncoding=utf-8";
        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "Root123!");

        // 使用SQLContext创建jdbc的DataFrame
        DataFrame dbDf = sqlsc.read().jdbc(url, "SYS_OP_LOG", prop);
        System.out.println(dbDf.collect());

        sc.close();
    }
}
