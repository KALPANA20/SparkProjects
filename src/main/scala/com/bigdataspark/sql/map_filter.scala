package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object map_filter {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("map_filter").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("map_filter").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data="file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\us-500.csv"
    val rdd=sc.textFile(data)
    val head=rdd.first()
    //val splitcoma = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"  // if any comma within double quotes ignore thats meaning
    //val process=rdd.filter(x=>x!=head).map(x=>x.split(splitcoma)).map(x=>(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(8).replaceAll("-","").replaceAll("\"","").toLong)).filter(x=>x._3>7735736914L)
    val process=rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(7).replaceAll("\"",""),1)).reduceByKey((a,b)=>a+b)
    process.take(10).foreach(println)

    spark.stop()
  }
}