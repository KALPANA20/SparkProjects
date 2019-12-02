package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object removespacefromheader {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("removespacefromheader").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("removespacefromheader").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val input= args(0)
    val tab= args(1)
    //val input="C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\10000SalesRecords.csv"
    val df=spark.read.format("csv").option("header","true").option("inferschema","true").load(input)
    //remove special characters from dataset
    val clean=df.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+",""))
    val ndf=df.toDF(clean:_*) //toDf rename all columns ..._* means all columns
    ndf.printSchema()
    ndf.show()
    val murl="jdbc:mysql://cts.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:3306/mysqldb"
    val prop=new java.util.Properties()
    prop.setProperty("user","musername")
    prop.setProperty("password","mpassword")
    prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    //ndf.write.jdbc(murl,"10000salesrecords",prop)
    ndf.write.jdbc(murl,tab,prop)

    spark.stop()
  }
}