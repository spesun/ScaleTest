package com.test.spark

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext

object GuijiMysql {

  def main(args: Array[String]): Unit = {

    var sparkConf = new SparkConf().setMaster("local").setAppName("MultiDataSource");
    var sc = new JavaSparkContext(sparkConf);
    var sqlsc = new SQLContext(sc);
    var url="jdbc:mysql://kf-master-db:3306/kf-workorder?useUnicode=true&characterEncoding=utf-8";
    var prop = new Properties();
    prop.put("user", "root")
    prop.put("password", "Root123!")

    // 使用SQLContext创建jdbc的DataFrame
    var dbDf = sqlsc.read.jdbc(url, "SYS_OP_LOG", prop);
    dbDf.foreach(line => {
      var rs = line.getAs[String]("RESULT")
      println(rs)
    })
//    println(dbDf.collect().toList)
    sc.close()
  }

}
