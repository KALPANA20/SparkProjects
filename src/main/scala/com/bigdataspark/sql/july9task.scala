package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object july9task {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("july9task").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("july9task").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
   //val data2="file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\us-500.csv"
    val data2=args(0) //args(0) means input data
    val op=args(1)//args(1) means output data
    val r1=sc.textFile(data2)
    val hd=r1.first()
    val splitcoma = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val res=r1.filter(x=>x!=hd).map(x=>x.split(splitcoma)).map(x=>(x(6).replaceAll("\"",""),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)

    res.take(10).foreach(println)
    res.saveAsTextFile(op)

    spark.stop()
  }
}