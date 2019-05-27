package com.test.spark

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


//江苏联通分析
object  Jiangsuliantong {

  def main(args: Array[String]): Unit = {
    GuijiMysql.setProp()

    val localpath = """C:/Users/admin/Desktop/t2"""
    val spark = SparkSession .builder() .appName("Spark SQL basic example") .master("local") .getOrCreate()
    //读csv文件
    val rootDf: DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "false") //在csv第一行有属性"true"，没有就是"false"
//      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .option("delimiter","|")
      .load(localpath)

    import spark.implicits._
    import org.apache.spark.sql.functions.schema_of_json
    import org.apache.spark.sql.functions.from_json

    var phone = "_c0"
    var url = "_c1"
    var request = "_c2"
    var response = "_c3"

    //处理c类用户
    var  inSchema = StructType(Seq(
      StructField("flag", StringType, false),
      StructField("productId", StringType, false),
      StructField("effectMode", StringType, false)
    ))
    var requestSchema = StructType(Seq(
      StructField("param", inSchema, false)
    ))
    var responseSchema = StructType(Seq(
      StructField("ErrorCode", StringType, false),
      StructField("ErrorMsg", StringType, false)
    ))
    var cDf = rootDf.withColumn(response, from_json(rootDf.col(response), responseSchema))
        .withColumn(request, from_json(rootDf.col(request), requestSchema))
      .where(s"${url}='/IntegrateService/UniformOrder/' ");
    cDf = cDf.select(phone, url, s"${request}.param.flag", s"${request}.param.productId",s"${request}.param.effectMode",s"${response}.ErrorCode", s"${response}.ErrorMsg")
    cDf.show(1000,false)


    //b类用户的请求是加密过的。
    inSchema = StructType(Seq (
      StructField("RESP_CODE", StringType, false),
      StructField("RESP_DESC", StringType, false)
    ) )
    responseSchema = StructType(Seq(
      StructField("UNI_BSS_HEAD", inSchema, false)
    ))
    var bdf = rootDf.withColumn(response, from_json(rootDf.col(response), responseSchema)).where(s"${url}='/aaop/SSIMPLENEWNew' ").
      select(phone, url, s"${response}.UNI_BSS_HEAD.RESP_CODE", s"${response}.UNI_BSS_HEAD.RESP_DESC")
    bdf.show(1000,false)
  }

}
