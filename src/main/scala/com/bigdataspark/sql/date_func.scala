package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object date_func {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("date_func").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("date_func").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val df = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
   //get current date
    //val ndf=df.withColumn("today",current_date())
    //get current time stamp
    //val ndf=df.withColumn("today",current_timestamp())
    //ndf.show(false) will display the full line without .....

    //to_date is used to convert string/time_stamp format to date format
    //val ndf=df.withColumn("today",to_date(current_timestamp())).withColumn("hiredate",to_date(unix_timestamp($"hiredate","dd-MMM-yy").cast("timestamp")))

    //return number of days difference between two dates
    //val ndf=df.withColumn("today",to_date(current_timestamp())).withColumn("hiredate",to_date(unix_timestamp($"hiredate","dd-MMM-yy").cast("timestamp"))).withColumn("datediff",datediff($"today",$"hiredate"))


    //returns additional number of days to be added in current date
    //val ndf=df.withColumn("today",to_date(current_timestamp())).withColumn("hiredate",to_date(unix_timestamp($"hiredate","dd-MMM-yy").cast("timestamp"))).withColumn("datediff",datediff($"today",$"hiredate")).withColumn("after100days",date_add($"today",100))

    //here -100 means 100 days before use like this or use date_sub()
    //val ndf=df.withColumn("today",to_date(current_timestamp())).withColumn("hiredate",to_date(unix_timestamp($"hiredate","dd-MMM-yy").cast("timestamp"))).withColumn("datediff",datediff($"today",$"hiredate")).withColumn("after100days",date_add($"today",-100))

    //dayofmonth gives a specified date from a month
      //dayofyear gives no.of days completed in a year
    //val ndf=df.withColumn("today",to_date(current_timestamp())).withColumn("hiredate",to_date(unix_timestamp($"hiredate","dd-MMM-yy").cast("timestamp"))).withColumn("datediff",datediff($"today",$"hiredate")).withColumn("after100days",date_add($"today",100)).withColumn("dayofyear",dayofyear($"hiredate"))


    //last_day func provides last day in a year
    //val ndf=df.withColumn("today",to_date(current_timestamp())).withColumn("hiredate",to_date(unix_timestamp($"hiredate","dd-MMM-yy").cast("timestamp"))).withColumn("datediff",datediff($"today",$"hiredate")).withColumn("after100days",date_add($"today",100)).withColumn("dayofyear",dayofyear($"hiredate")).withColumn("lastday",last_day($"hiredate"))


    //next_day gives the day like next sun ,sat etc...
    //val ndf1=ndf.withColumn("nextday",next_day($"today","Mon"))

    //unix_timestamp means

    spark.stop()
  }
}