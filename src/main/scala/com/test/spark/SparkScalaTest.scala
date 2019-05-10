package com.test.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json.Test

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

object  SparkScalaTest {

  def sumOfEveryPartition(input: Iterator[Int]): Int = {
    var total = 0
    println(total + 1)
    /*   input.foreach { (elem:Int) =>
          total = total + elem
        }
    input.foreach ((elem:Int) => println(elem + total) )
    */
    return total
  }

  def testMapPartitions(): Unit = {
    val conf = new SparkConf().setAppName("Spark Rdd Test").setMaster("local")
    val spark = new SparkContext(conf)
    val input = spark.parallelize(List(1, 2, 3, 4, 5, 6), 2)
    val result = input.mapPartitions(
      partition => Iterator(sumOfEveryPartition(partition)))

    result.collect().foreach {
      println(_)//6 15
    }

    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    GuijiMysql.setProp
//    testMapPartitions()
//    testReadFile()
//    testSaveFile()
    testRdd
  }


  def testSaveFile(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()
    import spark.implicits._

//    var file = spark.read.text("file:///tmp/spark/testData.txt")
//    file.write.text("file:///tmp/spark/testWrite.txt")



/*    var df = spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L)
    )).toDF("name", "age", "phone")

    df.show()*/

    var path = "/tmp/spark/testWrite.txt"
    GuijiMysql.delFile(path)

    var df = Seq((1,"brr"),(2,"hrr"),(3,"xxr")).toDF("id","name")

    var list = Seq(("11", 22))
    println(list)
//    df = spark.createDataFrame(list) //生成的是_1,_2的元组
    df = spark.createDataFrame(list).toDF("name", "age")
    df.write.json("file://" + path)


    spark.stop()
  }

  def  testRdd: Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    var rdd = spark.sparkContext.makeRDD(List(1,2,3))
    rdd.saveAsTextFile(("file://" + "/tmp/a"))

    var df = rdd.toDF()
    df.show()
    //
//    df.write.text("file://" + "/tmp/a")

  }


  def testReadFile(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testLocal")
    /*      .setMaster(args(0))
          .setAppName(args(1))
          .set("spark.executor.memory", "3g")*/
    val sc = new SparkContext(conf)
    val file = sc.textFile("file:///tmp/spark/testData.txt")
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    println(counts)
    println(counts.collect().toList)


  }

}
