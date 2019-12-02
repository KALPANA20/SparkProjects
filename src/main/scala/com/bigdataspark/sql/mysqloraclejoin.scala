package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object mysqloraclejoin {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("mysqloraclejoin").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("mysqloraclejoin").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    /*val msurl="jdbc:sqlserver://infy.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:1433;databaseName=sampledb"
    val msprop=new java.util.Properties()
    msprop.setProperty("user","msusername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")*/
    val murl="jdbc:mysql://cts.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:3306/mysqldb"
    val mprop= new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    val mdf=spark.read.jdbc(murl,"emp",mprop)


    val ourl="jdbc:oracle:thin:@//ibm.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop=new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val odf=spark.read.jdbc(ourl,"DEPTDATA",oprop)
    mdf.show()
    odf.show()
    odf.createOrReplaceTempView("dept")
    mdf.createOrReplaceTempView("emp")
    //val query="select e.ename,e.job,e.deptno,d.loc,d.dname from emp e join dept d on e.deptno=d.deptno"
    //val query=odf.join(mdf,$"deptno") //if column both are same
    //val query=odf.join(mdf,odf("deptno")===mdf("deptno")) //if columns are different
    val joindf=odf.join(mdf,$"deptno"===$"deptno")
    //val joindf=spark.sql(query)
    joindf.show()
    val tab=args(0)
    val s3path=args(1)
    joindf.write.mode(SaveMode.Overwrite).jdbc(murl,"tab",mprop)

     joindf.coalesce(1).write.format("csv").option("header","true").save(s3path)
    spark.stop()
  }
}