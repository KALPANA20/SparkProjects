package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._   //_ means import all

object sparkudf {
  def offer(SalesChannel:String)=SalesChannel match{
    case "Online" => "20%offer"
    case _ => "No offer"
  }
  val off=udf(offer _)
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("sparkudf").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkudf").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val input= "C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\10000SalesRecords.csv"
    val df=spark.read.format("csv").option("header","true").option("inferschema","true").load(input)
    val clean=df.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+",""))
    val ndf=df.toDF(clean:_*)
    //ndf.show()
    ndf.createOrReplaceTempView("tab")
    //val result=spark.sql("select concat_ws(' ',Region,Country) Place ,ItemType from tab")
    //val result=df.select(concat_ws("",$"Region",$"Country").alias("Place"))
    //val result=ndf.withColumn("test",lit("kalpana")) //withcolumn is mostly used for adding extra columns
         //lit is a method to add dummy values
    //val result=ndf.select(lit("dummy"), $"*") //if we want to insert column as first one
    //val result=ndf.withColumn("id",monotonically_increasing_id()+1)
    //val res=result.withColumnRenamed("id","idn")
    //update the original value
    //val result=ndf.withColumn("SalesChannel",when($"SalesChannel"==="Offline","online").otherwise($"SalesChannel")).withColumn("ItemType",when($"ItemType"==="Beverages","Drinks").otherwise($"ItemType"))
    //val result=ndf.withColumn("OrderDate",regexp_replace($"OrderDate","/","-"))


    //spark.udf.register("test",off)
    //val result=ndf.withColumn("offers",off($"SalesChannel"))
    //val result=spark.sql("select *,test(SalesChannel) offers from tab")
    val result=ndf.withColumn("ItemType",substring($"ItemType",3,10))
    result.show()
    /*val url="jdbc:oracle:thin:@//ibm.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val prop=new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    result.write.jdbc(url,"kalpanadata",prop)*/
    //result.coalesce(1).write.format("csv").option("header","true").save("C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\resnov11")
    spark.stop()
  }
}