package com.test.spark

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object  SparkScalaTest {
  var path = "/tmp/spark/testWrite.txt"
  var readFile = "/tmp/spark/testData.txt"

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
//    testRdd
//    testDataFrame
//    testDfSaveAsText()
    testThree()
  }


  def getSpark() : SparkSession = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    return spark
  }

  def testDataFrame(): Unit = {

    var spark = getSpark()
    //简单的例子
    var df = spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L)
    )).toDF("name", "age", "phone")

    df.show()

  }

  def testSaveFile(): Unit = {

//    var file = spark.read.text("file:///tmp/spark/testData.txt")
//    file.write.text("file:///tmp/spark/testWrite.txt")

//    不能使用。。 TODO
//    var spark = getSpark()

    val spark = SparkSession .builder() .appName("Spark SQL basic example") .master("local").getOrCreate()
    import spark.implicits._

    GuijiMysql.delFile(path)


    var list = Seq(("11", 22))
    println(list)
//    df = spark.createDataFrame(list) //生成的是_1,_2的元组
    var df = spark.createDataFrame(list).toDF("name", "age")
    df.write.json("file://" + path)



    spark.stop()
  }

  //必须放在外面。。。 TODO
  case class testcase(id:String)
  def testDfSaveAsText(): Unit = {
    val spark = SparkSession .builder() .appName("Spark SQL basic example") .master("local").getOrCreate()
    import spark.implicits._

    GuijiMysql.delFile(path)

    //var list = new Seq(("1")) 不能这样使用。。会自动转换成Seq("1")，导致不能创建df
    var list = Seq[testcase](testcase("1"))
    var df = spark.createDataFrame(list).selectExpr("concat_ws(',', id)").toDF()
    df.printSchema()
    df.show()
    //只有一列,且类型为string时，可以保存成功
    df = spark.createDataFrame(list).select("id").toDF()
    df.printSchema()
    df.show()
    df.write.text(path)


    import org.apache.spark.sql.functions._
     df = Seq((1,"brr"),(2,"hrr"),(3,"xxr")).toDF("id","name")
    //    var df = Seq((1,"brr"),(2,"hrr"),(3,"xxr")).toDS()
     df.show()
    //如果字段中没有数组，可以直接保存为csv
    //如果字段中有数组，需要将数组转换成字符串，才能保存成csv保存
/*      df.write.text(path)
      def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))
      df.withColumn("id", stringify(df("id"))).write.csv(path)*/

    //df无法直接保存为txt,需要将行转换成字符串
    /*    val allClumnName: String = df.columns.mkString(",")
        var splitRex = "|"
        val result: DataFrame = df.selectExpr(s"concat_ws('$splitRex',$allClumnName) as allclumn")
        result.write.text(path)*/


    spark.stop()
  }

  def  testRdd: Unit = {
    val spark = SparkSession .builder() .appName("Spark SQL basic example") .master("local") .getOrCreate()
    import spark.implicits._

    var path = "/tmp/spark/rdd"
    GuijiMysql.delFile(path)
    var rdd = spark.sparkContext.makeRDD(List(1,2,3))
    rdd.saveAsTextFile(("file://" + path))

    var df = rdd.toDF()
    df.show()
    //
//    df.write.text("file://" + "/tmp/a")

    spark.stop()
  }


  def testReadFile(): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testLocal")
    /*      .setMaster(args(0))
          .setAppName(args(1))
          .set("spark.executor.memory", "3g")*/
    val sc = new SparkContext(conf)
    val file = sc.textFile("file://" + readFile)
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    println(counts)
    println(counts.collect().toList)


  }

  def testThree(): Unit = {
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()
    import spark.implicits._
    //创建Rdd
    var seq = Seq("1")
    var rdd = spark.sparkContext.makeRDD(seq)  //java中无
    rdd = spark.sparkContext.parallelize(seq)
    rdd = spark.sparkContext.textFile(readFile)
//    spark.sparkContext.hadoopRDD()

//    创建DataFrame
    var df = seq.toDF("id")
    df = seq.toDF()
    var seq2 = Seq(("id", 1))
//    df = spark.createDataFrame(seq).  //异常
    df = spark.createDataFrame(seq2)

    var field = StructField("id", StringType, nullable = true)
    val schema = StructType(Seq(field))
//    df = spark.createDataFrame(seq, schema);


//    case class 可以直接就转成DS

  }
}
