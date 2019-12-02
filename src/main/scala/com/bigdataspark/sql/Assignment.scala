package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
case class diardd(c0:String,carat:String,cut:String,color:String,clarity:String,depth:String,table:String,price:String,x:String,y:String,z:String)
object Assignment {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("Assignment").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Assignment").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val res = "C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\diamonds.csv"
    val resdd=sc.textFile(res)
    val hd1=resdd.first()
    val pro1=resdd.filter(x=>x!=hd1).map(x=>x.split(",")).map(x=>diardd(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5),x(6),x(7),x(8),x(9),x(10)))
    val df=pro1.toDF()
    //val df = spark.read.format(source = "csv").option("header","true").option("inferschema","true").load(res)
    //df.show()
    //df.printSchema()
    df.createOrReplaceTempView(viewName = "tab")
    //val script=spark.sql(sqlText = "select avg(carat) from tab")
    //val script=spark.sql(sqlText = "select * from tab where carat>0.7 order by carat asc")
     //val script=spark.sql("select price from tab order by price desc limit 10")
    val script=spark.sql("select max(price) from tab")
    script.show()
    //val res1= df.sort(col("price").desc).limit(10)
    //df.show()

    spark.stop()
  }
}