import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object rddexp {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("rddexp").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("rddexp").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data="file:///C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\2008.csv"
    val rdd=sc.textFile(data)
    //map means apply a logic on top of all element
    //all transformations must use x=>x that means x takes each and every element
    //split is used to process the data separtely ie) it returns array of strings
    val head=rdd.first() //to display the first line
    val process=rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>x(0))
    //val process=rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2).toInt)).filter(x=>x._3>2 && x._3<4)
    process.take(10).foreach(println)


    spark.stop()
  }
}