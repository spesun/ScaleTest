package com.test.spark

import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
import util.control.Breaks._

object GuijiMysql {

  def setProp() {
    System.setProperty("hadoop.home.dir", "D:\\work\\hadoop\\hadoop-2.8.1")
  }

  case class DeviceIoTData (REQ_TIME:String, PARAMS:String, RESULT:String)

  def test(): Unit = {
    setProp()

    //    var sparkConf = new SparkConf().setMaster("local").setAppName("MultiDataSource");
    //    var sc = new SparkContext(sparkConf);
    //    var sqlsc = new SQLContext(sc);

    val spark = SparkSession .builder() .appName("Spark SQL basic example") .master("local") .getOrCreate()
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


    //注：这里面不能这样传递。。因为是在不同的线程中执行的,除非使用take
    //    var list = ArrayBuffer[String]()
    //    ds.take(Integer.MAX_VALUE).map(line => {
    /*    var newDs = ds.map(line => {
          line
        })*/

    var newDs = ds.map(line => {
      var result = line.RESULT
      //      dealResult(result)

      var list = ArrayBuffer[String]()
      var params = line.PARAMS
      if (params != null && !params.isEmpty) {
        val jsonS = scala.util.parsing.json.JSON.parseFull(params)
        var d = jsonS.get.asInstanceOf[List[_]]
        d.foreach(t => {
          if (t  != null) {
            println(t.toString)
            list += t.toString
          }
        })

      }

      //注：这里不能使用return. 否则编译报错 TODO
      list
    })

    var path = "/tmp/spark/test.json"
    delFile(path)

    newDs.write.json("file://" + path)
    //    spark.createDataset(list).toDF().write.json("file://" + path)
    spark.stop()
  }

  def getCeshi(): (String, Properties)= {
    var url="jdbc:mysql://kf-master-db:3306/kf-workorder?useUnicode=true&characterEncoding=utf-8";
    var prop = new Properties();
    prop.put("user", "root")
    prop.put("password", "Root123!")
    (url, prop)
  }

  def getOnline(): (String, Properties)= {
    var url="jdbc:mysql://ser.guiji.ai:3306/kf-workorder?useUnicode=true&characterEncoding=utf-8";
    var prop = new Properties();
    prop.put("user", "root")
    prop.put("password", "Root123!")
    (url, prop)
  }

  def main(args: Array[String]): Unit = {
//    test()
    setProp()

    val spark = SparkSession .builder() .appName("Spark SQL basic example") .master("local") .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    var tmp = getCeshi()
    var tmp = getOnline()
    var url = tmp._1
    var prop = tmp._2

    import spark.implicits._
    import org.apache.spark.sql.functions._

    var productConvert = "case  PRODUCT_VERSION when '02' then '01' else PRODUCT_VERSION end as PRODUCT_VERSION  ";
    var num = "case  PRODUCT_VERSION when '03' then '03' else '04' end as num ";

    //购买
    var buyInfos = spark.read.jdbc(url, "AGENT_BUY_INFO", prop).selectExpr("AGENT_ID",  "BOT_SENTENCE_NUM", productConvert, "MACHINE_NUM*VALID_TIME as MACHINE_NUM_DAY").
                        where("DELETE_FLAG='N'").groupBy("AGENT_ID", "PRODUCT_VERSION").
                          agg(sum("MACHINE_NUM_DAY").as("MACHINE_NUM_DAY") , sum("BOT_SENTENCE_NUM").as("BOT_SENTENCE_NUM"))
    buyInfos.show()

    //销售
    var sellInfos = spark.read.jdbc(url, "AGENT_SELL_INFO", prop).selectExpr("AGENT_ID", productConvert, "BOT_SENTENCE_NUM", "MACHINE_NUM*VALID_TIME as MACHINE_NUM_DAY").
                  where("DELETE_FLAG='N'"). groupBy("AGENT_ID", "PRODUCT_VERSION").//sum("MACHINE_NUM_DAY", "BOT_SENTENCE_NUM")
                    agg(sum("MACHINE_NUM_DAY").as("MACHINE_NUM_DAY") , sum("BOT_SENTENCE_NUM").as("BOT_SENTENCE_NUM"))
    sellInfos.show()

    //汇总
/*    var sellList = sellInfos.collectAsList()
    FileUtils.writeStringToFile(new java.io.File("/tmp/spark/sell"), sellList.toString)*/

    //agg如何使用
//    var sumInfos = spark.read.jdbc(url, "AGENT_SUM_INFO", prop).groupBy("AGENT_ID", "PRODUCT_VERSION").sum("USE_MACHINE_DAY_NUM", "TOTAL_MACHINE_DAY_NUM")
    var sumInfos = spark.read.jdbc(url, "AGENT_SUM_INFO", prop).select($"AGENT_ID", $"PRODUCT_VERSION", $"USE_MACHINE_DAY_NUM", $"TOTAL_MACHINE_DAY_NUM");
    sumInfos.show()
/*    var sumList = sumInfos.collectAsList()
    FileUtils.writeStringToFile(new java.io.File("/tmp/spark/sum"), sumList.toString)*/


    sellOrBuy(buyInfos, sumInfos, "TOTAL_MACHINE_DAY_NUM")
//    sellOrBuy(sellInfos, sumInfos, "USE_MACHINE_DAY_NUM")

    spark.stop()
  }

  def sellOrBuy(df: DataFrame, sumInfos:DataFrame, sumField:String): Unit = {

    //需要使用``转义，否则当成sum函数了
    //机器人
    var sell = df.selectExpr("AGENT_ID",  "MACHINE_NUM_DAY as num").where("PRODUCT_VERSION in ('01', '02')")
    var sellSum = sumInfos.selectExpr("AGENT_ID",  s"${sumField} as num").where("PRODUCT_VERSION='04'")
    var sellIntersectList1 = sell.intersect(sellSum).collectAsList()
    var sellExceptList1 = sell.except(sellSum).collectAsList()
    println("机器交集")
    println(sellIntersectList1)
    println("机器差集")
    println(sellExceptList1)

    //话术
    sell = df.selectExpr("AGENT_ID",  "BOT_SENTENCE_NUM as num").where("PRODUCT_VERSION in ('03')")
//    sell.show()
    sellSum = sumInfos.selectExpr("AGENT_ID",  s"$sumField as num").where("PRODUCT_VERSION='03'")
//    sellSum.show()
    var sellIntersectList2 = sell.intersect(sellSum).collectAsList()
    var sellExceptList2 = sell.except(sellSum).collectAsList()

    println("话术交集")
    println(sellIntersectList2)
    println("话术差集")
    println(sellExceptList2)

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
