package com.test.spark

import java.util

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

/**
  * Created by shirukai on 2018/7/17
  * 创建DataFrame的几种方式
  */
object CreateDataFrameTest {
  def main(args: Array[String]): Unit = {
    testThree()
    createDfAndDs()
  }

  def testThree(): Unit = {
    case class testcase(id:String)
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()
    import spark.implicits._
    //注：Seq中的字符串不是Product的子类
    var seq = Seq("1")
    //注：Seq中的元组是字符串的子类
    var seq2 = Seq(("id", 1))
    var seqcase = Seq(testcase("1"))

    //创建Rdd
    var rdd = spark.sparkContext.makeRDD(seq)  //java中无
    rdd = spark.sparkContext.parallelize(seq)
    rdd = spark.sparkContext.textFile(SparkScalaTest.readFile)
    //    spark.sparkContext.hadoopRDD()

    // 创建DataFrame
    var df = seq.toDF("id")
    df = seq.toDF()
    //    df = spark.createDataFrame(seq).  //异常
    df = spark.createDataFrame(seq2)

    //使用caseclass
    df = spark.createDataFrame(spark.sparkContext.makeRDD(seq), testcase.getClass);

    //使用row, 必须是java List。或Java RDD
    import scala.collection.JavaConverters._
    val rowRDD = seq2.map(p => Row(p)).toList.asJava
    var field = StructField("id", StringType, nullable = true)
    val schema = StructType(List(field))
    println(rowRDD.getClass)
    df = spark.createDataFrame(rowRDD, schema);

  }


  def createDfAndDs(): Unit = {
        val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName).master("local")
      .getOrCreate()

    //第一种：通过Seq生成
    val df = spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L)
    )) toDF("name", "age", "phone")

    df.show()

    //第二种：通过读取Json文件生成
    val dfJson = spark.read.format("json").load("/Users/shirukai/Desktop/HollySys/Repository/sparkLearn/data/student.json")
    dfJson.show()

    //第三种：通过读取Csv文件生成
    val dfCsv = spark.read.format("csv").option("header", true).load("/Users/shirukai/Desktop/HollySys/Repository/sparkLearn/data/students.csv")
    dfCsv.show()

    //第四种：通过Json格式的RDD生成（弃用）
    val sc = spark.sparkContext
    import spark.implicits._
    val jsonRDD = sc.makeRDD(Array(
      "{\"name\":\"ming\",\"age\":20,\"phone\":15552211521}",
      "{\"name\":\"hong\", \"age\":19,\"phone\":13287994007}",
      "{\"name\":\"zhi\", \"age\":21,\"phone\":15552211523}"
    ))

    val jsonRddDf = spark.read.json(jsonRDD)
    jsonRddDf.show()

    //第五种：通过Json格式的DataSet生成
    val jsonDataSet = spark.createDataset(Array(
      "{\"name\":\"ming\",\"age\":20,\"phone\":15552211521}",
      "{\"name\":\"hong\", \"age\":19,\"phone\":13287994007}",
      "{\"name\":\"zhi\", \"age\":21,\"phone\":15552211523}"
    ))
    val jsonDataSetDf = spark.read.json(jsonDataSet)

    jsonDataSetDf.show()

    //第六种: 通过csv格式的DataSet生成
    val scvDataSet = spark.createDataset(Array(
      "ming,20,15552211521",
      "hong,19,13287994007",
      "zhi,21,15552211523"
    ))
    spark.read.csv(scvDataSet).toDF("name","age","phone").show()

    //第七种：动态创建schema
    val schema = StructType(List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("phone", LongType, true)
    ))
    val dataList = new util.ArrayList[Row]()
    dataList.add(Row("ming",20,15552211521L))
    dataList.add(Row("hong",19,13287994007L))
    dataList.add(Row("zhi",21,15552211523L))
    spark.createDataFrame(dataList,schema).show()

    //第八种：读取数据库（mysql）
    val options = new util.HashMap[String,String]()
    options.put("url", "jdbc:mysql://localhost:3306/spark")
    options.put("driver","com.mysql.jdbc.Driver")
    options.put("user","root")
    options.put("password","hollysys")
    options.put("dbtable","user")

    spark.read.format("jdbc").options(options).load().show()
  }
}

