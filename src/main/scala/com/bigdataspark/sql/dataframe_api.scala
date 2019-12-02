package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object dataframe_api {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("dataframe_api").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("dataframe_api").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val dt="file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\us-500.csv"
    val df2=spark.read.format(source = "csv").option("header","true").option("inferschema","true").load(dt)
    df2.show()
    df2.printSchema()

    spark.stop()
  }
}