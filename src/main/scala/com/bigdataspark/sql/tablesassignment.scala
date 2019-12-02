package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object tablesassignment {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("tablesassignment").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("tablesassignment").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    /*val ourl="jdbc:oracle:thin:@//ibm.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop=new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")*/
    val input="C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\new-york-city-current-job-postings\\nyc-jobs.csv"
    val df=spark.read.format("csv").option("header","true").option("inferschema","true").option("delimiter",",").load(input)
    val msurl="jdbc:sqlserver://infy.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:1433;databaseName=sampledb"
    val msprop=new java.util.Properties()
    msprop.setProperty("user","msusername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    //val df=spark.read.jdbc(ourl,"BANKDATA",oprop)
    df.write.mode(SaveMode.Overwrite).jdbc(msurl,"kalpananydata",msprop)
    df.write.jdbc(msurl,"kalpananydata",msprop)
    //val clean=df.columns.map(x=>x.replaceAll("[^\\p{L}\\p{Nd}]+",""))
    spark.stop()
  }
}