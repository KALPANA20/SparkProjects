package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object datasetjson {
  case class zipcc(
                    city: String,
                    loc: List[Double],
                    pop: Double,
                    state: String,
                    _id: String
                  )
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("datasetjson").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("datasetjson").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data="C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\zips.json"
    val df=spark.read.format("json").load(data)
    val ds=df.as[zipcc]
    ds.show()
    ds.createOrReplaceTempView("tab")
    val res=spark.sql("select _id id,city,loc[0] lattitude,loc[1] longitude,pop,state from tab")
    res.show(5)

    spark.stop()
  }
}