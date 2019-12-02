package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object wordcount {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("wordcount").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val word= "file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\wordcount.txt"
    val wrdd =sc.textFile(word)
    val result=wrdd.flatMap(x=>x.split(" ")).map(x=>(x.toLowerCase(),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    result.take(10).foreach(println)
    //result.saveAsTextFile(path = "C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\output")

    spark.stop()
  }
}