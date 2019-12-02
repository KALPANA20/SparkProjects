package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object datasetapi {
  case class bankcc(age:Int,job:String,marital:String,education:String,defaultl:String,balance:Int,housing:String,loan:String,contact:String,day:String,month:String,duration:String,campaign:String,pdays:String,previous:String,poutcome:String,y:String)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("datasetapi").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("datasetapi").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data="file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\bank-full.csv"
    val df=spark.read.format("csv").option("header","true").option("delimiter",";").option("inferschema","true").load(data).withColumnRenamed("default","defaultl")
    val ds=df.as[bankcc]
    ds.createOrReplaceTempView("tab")
    val res=spark.sql("select job,count(*) cnt from tab group by job")

    res.show()

    spark.stop()
  }
}