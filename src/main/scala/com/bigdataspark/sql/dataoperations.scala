package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object dataoperations {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("dataoperations").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("dataoperations").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val input = "C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\bank-full.csv"
    val df = spark.read.format(source = "csv").option("header","true").option("delimiter",";").option("inferschema","true").load(input)
    df.select("*").where($"job"==="retired" && $"age".gt(50)).show(3)
    //where and filter are exactly same
    df.select("*").filter($"job"==="retired" && $"age".gt(50)).show(3)
    //updating a value by creating a new dataframe
    //val udf=ndf.withColumn("cut",when($"cut"==="Very Good","Excellent").otherwise($"cut"))
    //ndf.groupBy($"cut").count.orderBy($"count".desc).show
    df.show()

    df.printSchema()
    spark.stop()
  }
}