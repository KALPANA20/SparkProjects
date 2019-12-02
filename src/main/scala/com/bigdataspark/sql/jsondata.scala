package com.bigdataspark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
//json data is light weight data format
object jsondata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("jsondata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("jsondata").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    /*val data="C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\zips.json"
    val df=spark.read.format("json").option("header","true").option("inferschema","true").load(data)
     df.show(5,false)
    df.printSchema()
    //here we have array of data.Array is not supported while storing in database so we have to make it structured data
    df.createOrReplaceTempView("tab")
    val result=spark.sql("select  _id as id,city,loc[0] longitude,loc[1] latitude,pop,state from tab")
    result.show()
    result.printSchema() */
    val data="C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\world_bank.json"
    val df=spark.read.format("json").option("header","true").option("inferschema","true").load(data)
    df.show(5,false)
    df.printSchema()
    df.createOrReplaceTempView("tab")
    //if only array use arr[0],arr[1]....
    //whereas if we have only struct as datatype use parent column.child column
    //array is used for same datatype and struct allows multiple datatypes
    //if column contains special characters like $,#,! at that time to ignore these we are using ``(on tilde~ symbol)
    //val query="select _id.`$oid` id,theme1.Name themename,theme1.Percent themepercent from tab"
    //use lateral view explode when we have complex datatypes like struct inside array
    val query="select _id.`$oid` id,theme1.Name themename,theme1.Percent themepercent,mp.Name,mp.Percent from tab lateral view explode(majorsector_percent) a as mp"
    val query1="""select  _id.`$oid` OID, approvalfy,board_approval_month,boardapprovaldate, borrower,closingdate,country_namecode,countrycode, supplementprojectflg,countryname,countryshortname,envassesmentcategorycode,grantamt,ibrdcommamt,id,idacommamt,impagency,lendinginstr, lendinginstrtype,lendprojectcost, mjthemecode,sector.name[0] name1,sector.name[1] name2, sector.name[2] name3,sector1.name S1Name, sector1.percent S1Percent,sector2.name S2Name, sector2.percent S2Percent,sector3.name S3Name, sector3.percent S3Percent,sector4.name S4Name, sector4.percent S4Percent, prodline, prodlinetext, productlinetype,mjtheme[0] mjtheme1,mjtheme[1] mjtheme2,mjtheme[2] mjtheme3  , project_name ,projectfinancialtype, projectstatusdisplay,regionname,sectorcode, source,status,themecode,totalamt,totalcommamt,url,mp.Name mpname, mp.Percent mppercent, tn.code tncode, tn.name tnname,mn.code mncode, mn.name mnname, tc.code tccode, tc.name tcname, pd.DocDate pddocdate ,pd.DocType pddoctype, pd.DocTypeDesc pddoctypedesc, pd.DocURL  pddocurl, pd.EntityID pdid from tab lateral view explode(majorsector_percent) tmp as mp lateral view explode(theme_namecode) tmp as tn lateral view explode(mjsector_namecode) tmp as mn lateral view explode(mjtheme_namecode) tmp as tc lateral view explode(projectdocs) tmp as pd"""
    val res=spark.sql(query1)
    res.createOrReplaceTempView("temp")
    //val fres=spark.sql("select countrycode,count(*) cnt from temp group by countrycode order by cnt desc")
    val fres=res.select($"countrycode").groupBy($"countrycode").count().orderBy($"count".desc)
    fres.show()
    fres.coalesce(1).write.format("csv").option("header","true").save("hdfs://localhost:9000/result")

    spark.stop()
  }
}