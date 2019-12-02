package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

object CassandraAss {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("CassandraAss").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("CassandraAss").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val ourl="jdbc:oracle:thin:@//ibm.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop=new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val odf=spark.read.jdbc(ourl,"EMP",oprop)
    odf.createOrReplaceTempView("tab")
    val res=spark.sql("select EMPNO empno,ENAME ename,JOB job,MGR mgr,HIREDATE hiredate,SAL sal,COMM comm,DEPTNO deptno from tab")

    res.show()
    res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","emp").option("keyspace","kalpana_db").save()
    /*val murl="jdbc:mysql://cts.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:3306/mysqldb"
    val mprop=new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    val mdf=spark.read.jdbc(murl,"dept",mprop)
    mdf.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","dept").option("keyspace","kalpana_db").save()*/


    spark.stop()
  }
}