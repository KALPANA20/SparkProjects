import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object rddass {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[]").appName("rddass").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("rddass").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data1="C:\\Users\\KalpanaAnandan\\Desktop\\BigdataClass\\Dataset\\bank.csv"
    val r=sc.textFile(data1)
    val head1=r.first()
    //val pro=r.filter(x=>x!=head1).map(x=>x.split(";")).map(x=>(x(0).toInt,x(2)))
    //println(pro.max())
    val pro=r.filter(x=>x!=head1).map(x=>x.split(";")).map(x=>(x(0).toInt,x(1).replaceAll("-",""),x(2)))
    //pro.take(10).foreach(println)

    spark.stop()
  }
}