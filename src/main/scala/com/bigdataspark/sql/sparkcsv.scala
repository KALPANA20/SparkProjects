package com.bigdataspark.sql
//sparkcontext -> to create rdd
//sqlcontext -> to create dataframe
//sparksession -> spark....to create dataset API
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkcsv {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("sparkcsv").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkcsv").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    val input = "C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\us-500.csv"
     val df = spark.read.format("csv").option("header", "true") .option("inferschema","true").load(input)
    //df.show() //by default shows first 20 records
    //df.printSchema()
    //sql friendly
    df.createOrReplaceTempView(viewName = "tab") //it allows to run the sql queries on top of dataframes
    //val res = spark.sql(sqlText = "select state,count(*) cnt from tab group by state order by cnt desc")
    //val res = spark.sql(sqlText = "select * from tab where zip =(select max(zip) from tab)")
    val res= spark.sql(sqlText = "select first_name,regexp_replace(phone1,'-','')as phone1,regexp_replace(phone2,'-','')as phone2 from tab")
    res.show()
    val op= "hdfs://localhost:9000/result"
    res.write.format(source = "csv").option("header","true").option("delimiter", "|").save(op)
    //scala friendly
    //val res= df.groupBy(cols=$"state").count().orderBy($"count" .desc)
    //res.show()
    spark.stop()
  }
}