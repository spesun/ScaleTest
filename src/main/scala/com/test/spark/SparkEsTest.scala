package com.test.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkEsTest {

  def testSelect(): Unit = {

    val conf = new SparkConf().setAppName("test es").setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "ai72")
    conf.set("es.port", "9200")

    var sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc)

    // options for Spark 1.3 need to include the target path/resource
    val options13 = Map("path" -> "test",
      "pushdown" -> "true",
      "es.nodes" -> "ai72","es.port" -> "9200")

    // Spark 1.3 style
    val spark13DF = sqlContext.load("org.elasticsearch.spark.sql", options13)

    // options for Spark 1.4 - the path/resource is specified separately
    val options = Map("pushdown" -> "true", "es.nodes" -> "10.1.235.72", "es.port" -> "9200")

    // Spark 1.4 style
    val spark14DF = sqlContext.read.format("org.elasticsearch.spark.sql").options(options).load("test")

    // ��ѯname,age:
    //spark14DF.select("name","age").collect().foreach(println(_))

    // ע����ʱ����ѯname
    spark14DF.registerTempTable("people")
    var results = sqlContext.sql("SELECT name FROM people")
    //results.map(t => "Name: " + t(0)).collect().foreach(println)


    /** spark.sql�� */
    val sql = new SQLContext(sc);
    //val people = sql.esDF("test", "?q=john");
    val people = sql.esDF("test", "?q=name:j*hn OR name:/test[a-T]/");
    //people.show();

    //��Сд����
    people.registerTempTable("test")
    results = sql.sql("SELECT distinct name FROM test limit 4")
    results.map(t => "Name: " + t(0)).collect().foreach(println)

    results = sql.sql("SELECT name FROM test ")
    results.map(t => "Name: " + t(0)).collect().foreach(println)

    //results = sql.sql("SELECT distinct name FROM test")
    //results.map(t => "Name: " + t(0)).collect().foreach(println)
    //println(people.schema.treeString)
  }

  def testInsert() = {
    val conf = new SparkConf().setAppName("test es")//.setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "ai72")
    conf.set("es.port", "9200")
/*    conf.set("keytab", "/home/client/client.keytab")
    conf.set("principal", "client/dcp@DCP.COM")
    conf.set("spark.authenticate", "true")*/

    val sc = new org.apache.spark.SparkContext(conf);

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    var rs = hiveContext.sql("select name , male , bir , age ,add from testes " )
    rs.show();

    /** ͨ��df��ʽ���� */
    rs.saveToEs("test/user")


    /** insert select ��ʽ, ��Ҫע��select �е�˳��. bir���� */
    val options = Map("pushdown" -> "true", "es.nodes" -> "10.1.235.72", "es.port" -> "9200")
    // Spark 1.4 style
    val spark14DF = hiveContext.read.format("org.elasticsearch.spark.sql").options(options).load("test/user")
    spark14DF.show()
    // ע����ʱ����ѯname
    spark14DF.registerTempTable("people")
    //hiveContext.sql("insert into table people select  add , age ,bir, male, name  from testes " )
  }

  def testInsertBj(table :String ) = {
    val conf = new SparkConf().setAppName("test es");//.setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "10.221.1.153")
    conf.set("es.port", "9200")

    val sc = new org.apache.spark.SparkContext(conf);

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    var rs = hiveContext.sql("select * from " + table )
    //rs.show();

    /** ͨ��df��ʽ���� */
    rs.saveToEs(table)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    //testSelect();
    testInsert();
    //var table = args(0);
    //testInsertBj(table);
  }

}
