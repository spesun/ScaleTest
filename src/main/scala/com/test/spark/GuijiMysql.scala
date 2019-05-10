package com.test.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
import util.control.Breaks._

object GuijiMysql {

  def setProp() {
    System.setProperty("hadoop.home.dir", "D:\\work\\hadoop\\hadoop-2.8.1")
  }

  case class DeviceIoTData (REQ_TIME:String, PARAMS:String, RESULT:String)

  def main(args: Array[String]): Unit = {
  setProp()

    //    var sparkConf = new SparkConf().setMaster("local").setAppName("MultiDataSource");
    //    var sc = new SparkContext(sparkConf);
    //    var sqlsc = new SQLContext(sc);

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    var url="jdbc:mysql://kf-master-db:3306/kf-workorder?useUnicode=true&characterEncoding=utf-8";
    var prop = new Properties();
    prop.put("user", "root")
    prop.put("password", "Root123!")

    //例子
    /*    var df1 = session.read
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/smvc")
          .option("dbtable", "user")
          .option("user", "root")
          .option("pass" + "word", "123456")
          .load();*/

    import spark.implicits._
    // 使用SQLContext创建jdbc的DataFrame
    var dbDf = spark.read.jdbc(url, "SYS_OP_LOG", prop).where("STATUS='Y'")
    var ds = dbDf.as[DeviceIoTData];


    //TODO 大了会自动清空？
    var list = ArrayBuffer[String]()
    ds.take(10).foreach(line => {
      var result = line.RESULT
//      dealResult(result)

      var params = line.PARAMS
      if (params != null && !params.isEmpty) {
        val jsonS = scala.util.parsing.json.JSON.parseFull(params)
        var d = jsonS.get.asInstanceOf[List[_]]
        d.foreach(t => {
          if (t  != null) {
//            println(t.toString)
            list += t.toString
            println(list)
          }
        })
      }

    })

    println(list)
    var path = "/tmp/spark/test.json"
    delFile(path)
    spark.createDataset(list).toDF().write.json("file://" + path)
    spark.stop()
  }

  def delFile(path:String): Unit = {
    var file = File(path)
    if (file.exists) {
      file.deleteRecursively()
    }
  }

  def dealResult(result:String): Unit = {
    if (result != null) {
      val jsonS = scala.util.parsing.json.JSON.parseFull(result)
      jsonS match {
        case Some(map: Map[String, Any]) => {
          //            map.filter(_._2.equals("000000")).foreach(println(_))
          //            println(map.get("rspCode"))
          if (map.get("rspCode").get.equals("000000")) {
            var a = map.get("data").getOrElse("ignore")
            if(!a.equals("ignore")) {
              //                println(a)
              if(a.isInstanceOf[Map[_,_]]) {
                println(" Map result = " + a)
              } else {
                println("string result = " + a )
              }

            }
          }
        }
        case None => println("not json " + result)
        case other => throw new IllegalArgumentException
      }
    }
  }

  @Deprecated
  def regJson(json:Option[Any]) = {
    json match {
      case Some(map: Map[String, Any]) => map
      case None => Map("1" -> "2")
      case other => throw new IllegalArgumentException
    }
  }

}
