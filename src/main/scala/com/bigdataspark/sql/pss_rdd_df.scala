package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
//first method-->toDF
//second method-->case class
//third method-->programmatically
//fourth method-->dataframe api
//priority 4,3,2,1
//case class->very very small datasets
//pss-->large datasets
//but in production we use dataframe api
object pss_rdd_df {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("pss_rdd_df").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("pss_rdd_df").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //programmatically specifying schema
    //https://spark.apache.org/docs/2.3.1/sql-programming-guide.html#programmatically-specifying-the-schema
    val data4="file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\us-500.csv"
    val r3=sc.textFile(data4)
    val hd1=r3.first()
    val splitcoma = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    // The schema is encoded in a string
    val cols = "first_name,last_name,company_name,address,city,county,state,zip,phone1,phone2,email,web"
    // Generate the schema based on the string of schema
    val fields = cols.split(",").map(x => StructField(x, StringType, nullable = true))
    val schema = StructType(fields)
    // Convert records of the RDD (people) to Rows
    val rowRDD = r3.map(x=>x.split(splitcoma)).map(x=> Row(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2),x(3),x(4),x(5),x(6).replaceAll("\"",""),x(7),x(8),x(9),x(10),x(11)))
    // Apply the schema to the RDD
    val df1 = spark.createDataFrame(rowRDD, schema)
    df1.show(3)


    spark.stop()
  }
}