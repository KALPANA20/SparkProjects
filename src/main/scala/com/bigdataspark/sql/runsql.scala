package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
case class uscc(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:String,phone1:String,phone2:String,email:String,web:String)

object runsql {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("runsql").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("runsql").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data3="file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\us-500.csv"
    val r2=sc.textFile(data3)
    val hd1=r2.first()
    val splitcoma = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    /*val res=r2.filter(x=>x!=hd1).map(x=>x.split(splitcoma)).map(x=>(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2),x(3),x(4),x(5),x(6).replaceAll("\"",""),x(7),x(8),x(9),x(10),x(11)))
    val df =res.toDF("first_name","last_name","company_name","address","city","county","state","zip","phone1","phone2","email","web")
    //toDF if we are not mentioning column names then it displays as c0,c1)
    df.show()
    df.createOrReplaceTempView("tab")
    val res1 = spark.sql(sqlText = "select * from tab where state='MD'")
    res1.show()*/ //if you want to comment the likes use ctrl+shift+?
    //case class to convert rdd to dataframe.....case class must be out of main method
    val res=r2.filter(x=>x!=hd1).map(x=>x.split(splitcoma)).map(x=>uscc(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2),x(3),x(4),x(5),x(6).replaceAll("\"",""),x(7),x(8),x(9),x(10),x(11)))
    val df=res.toDF()
    df.show(3)
    spark.stop()
  }
}