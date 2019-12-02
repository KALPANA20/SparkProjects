package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object getoracledata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("getoracledata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("getoracledata").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    /*val url="jdbc:mysql://cts.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:3306/mysqldb"
    val prop= new java.util.Properties()
    prop.setProperty("user","musername")
    prop.setProperty("password","mpassword")
    prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    //val df=spark.read.jdbc(url,"dept",prop)
    //import based on query
    //val query="(select * from dept where loc='CHICAGO') ABCD"
    //val df=spark.read.jdbc(url,query,prop)
    val query="""(select e.empno,e.ename,d.deptno,d.loc from emp e,dept d where e.deptno=d.deptno and d.deptno>20) ABCD"""
    val df=spark.read.jdbc(url,query,prop)
    df.show()*/

   val data="C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\bank-full.csv"
    val df=spark.read.format("csv").option("header","true").option("inferschema","true").option("delimiter",";").load(data)
    df.show()
    val url="jdbc:oracle:thin:@//ibm.cdanbwqnw7lo.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val prop=new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    df.write.jdbc(url,"bankdata",prop)

    spark.stop()
  }
}
//Exception in thread "main" java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver
//dependency error