package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object hiveprocessing {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("hiveprocessing").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("hiveprocessing").enableHiveSupport().getOrCreate()
    //must use enablehivesupport to process hive in spark
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data= args(0)
    //val hivetab=args(1)
    val df=spark.read.format("csv").option("header","true").option("inferschema","true").load(data)
    df.createOrReplaceTempView("tab")
    val res=spark.sql("select year,month from tab")
    res.write.saveAsTable("ushivedata")

    spark.stop()
  }
}