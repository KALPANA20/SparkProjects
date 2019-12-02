package com.bigdataspark.sql

import java.sql.Struct

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.databricks.spark.xml._

object xmldata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("xmldata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("xmldata").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data="file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\xmldata.xml"
    val df=spark.read.format("xml").option("rowTag","course").load(data)
    df.show()
    df.printSchema()
    //val ds=df.as[xmlcc]
    df.createOrReplaceTempView("tab")
    val res=spark.sql("select * from tab")
    res.show(5)


    spark.stop()
  }
  //case class xmlcc(crse:Long, days:String, instructor:String, place:Struct, building:String, room:String, reg_num:Long, sect:String, subj:String, time:Struct, end_time:String, start_time:String, title:String, units:Double)
}