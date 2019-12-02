package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

object spark_cassandra {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("spark_cassandra").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("spark_cassandra").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //val df = spark.read.format("org.apache.spark.sql.cassandra").option("table","asl").option("keyspace","kalpana_db").load()
    //df.show()
    //df.createOrReplaceTempView("tmp")
    //val res=spark.sql("select name from tmp where id=11")
    val data="C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\us-500.csv"
    val df=spark.read.format("csv").option("header","true").option("inferschema","true").load(data)
     df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","usdata").option("keyspace","kalpana_db").save()
    //res.show()
    spark.stop()
  }
}