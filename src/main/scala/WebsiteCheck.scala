import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WebsiteCheck {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf
      .setMaster("local[*]")
      .setAppName("SparkTestJob")

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .config(conf)
      .getOrCreate()

    val rdd=spark.sparkContext.textFile("/home/binaya/Desktop/IPaddressCheckTop5.txt")
    val rdd1=rdd.map(t=>t.split("- -")(0)).map(t=>(t,1)).reduceByKey((a,b)=>a+b)
      .sortBy(_._2,ascending = false).collect()
    rdd1.take(5).foreach(println)



  }
}
