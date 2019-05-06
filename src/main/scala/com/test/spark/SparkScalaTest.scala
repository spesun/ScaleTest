package com.test.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.json.Test

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
    testMapPartitions()
    testFile()
  }

  def testFile(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testLocal")
    /*      .setMaster(args(0))
          .setAppName(args(1))
          .set("spark.executor.memory", "3g")*/
    val sc = new SparkContext(conf)
    val file = sc.textFile("file:///tmp/spark/testData.txt")
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    println(counts.collect().toList)
  }

}
