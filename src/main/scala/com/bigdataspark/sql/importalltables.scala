package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object importalltables {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("importalltables").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importalltables").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //reading data from mysql and exporting data in oracle
    val murl="jdbc:mysql://cts.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:3306/mysqldb"
    val mprop=new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    //val tbls=Array("BANKDATA","DEPTDATA")
    val query="(SELECT TABLE_NAME FROM ALL_TABLES WHERE TABLESPACE_NAME='USERS') ABC"
    val ourl="jdbc:oracle:thin:@//ibm.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop=new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val df1=spark.read.jdbc(ourl,query,oprop)
    //convert dataframe to rdd map get one by one table
    val tabs=df1.select("TABLE_NAME").rdd.map(x=>x(0)).collect.toArray
    //foreach applies logic on each element
    tabs.foreach{x=>
      println(s"importing table from $x")
      if (x=="BANKDATA") s"(select age,job from $x) ahfh"

      val df=spark.read.jdbc(ourl,s"$x",oprop)
      df.write.jdbc(murl,s"$x",mprop)
      //df.show()
    }

    //val df=spark.read.jdbc(murl,"dept",mprop)
    //df.write.mode(SaveMode.Overwrite).jdbc(ourl,"deptdata",oprop) -->overwrite the table if already exists in the target database
    //df.write.jdbc(ourl,"deptdata",oprop)



    spark.stop()
  }
}