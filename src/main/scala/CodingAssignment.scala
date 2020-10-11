import org.apache.spark.SparkContext
import org.apache.log4j.{ Logger }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object CodingAssignment extends App {
  val spark = SparkSession.builder.master("local[*]").appName("Synechron Coding Assignment").getOrCreate()
  val sc = spark.sparkContext
  /*case class Transaction(transactionId: String, accountId: String, transactionDay: Int, category: String, transactionAmount: Double)
  def main(args: Array[String]) {

    @transient lazy val logger = Logger.getLogger(getClass.getName)

    val name = "Synechron Coding Assignment"
    logger.info(s"Starting up $name")

    System.setProperty("hadoop.home.dir", "C:\\winutils_hadoop\\bin");

    import spark.implicits._

    val fileName = "/home/binaya/Documents/SnechronAssignment/transactions.txt"

    val transactionRdd = sc.textFile(fileName);

    val header = transactionRdd.first()

    val transactionWH = transactionRdd.filter(x => x != header)
   // val transactionWHh = transactionRdd.filter(x => x != header).foreach(println)


    val transactionsRDD = transactionWH.map(rec => {
      val r =
        rec.split(","); Transaction(r(0), r(1), r(2).toInt, r(3), r(4).toDouble)
    });

    val transactionsDF = transactionsRDD.toDF();
    transactionsDF.show()

    transactionsDF.createOrReplaceTempView("transaction_tbl")



    //Calculate the total transaction value for all transactions for each day.
    spark.sql("select transactionDay,sum(transactionAmount) from transaction_tbl group by transactionDay order by transactionDay").show

    //spark.sql("select distinct category from transaction_tbl  order by category").show

    //Calculate the average value of transactions per account for each type of transaction (there are seven in total).
    //spark.sql("select category,accountId,avg(transactionAmount) from transaction_tbl group by accountId, category order by accountId").show(100)
    spark.sql("select category,accountId,avg(transactionAmount) from transaction_tbl group by accountId, category order by accountId").count()

    //The maximum transaction value in the previous 5 days of transactions per account
    //The average transaction value of the previous 5 days of transactions per account
    val query="select transactionDay,accountId,max(transactionAmount),avg(transactionAmount), sum(transactionAmount) "+
      "from transaction_tbl where transactionDay<=5 group by accountId,transactionDay order by accountId"

    spark.sql(query).show

    //The total transaction value of transactions types “AA”, “CC” and “FF” in the previous 5 days per account
    val totalTransactionValue = transactionsDF.groupBy(col("accountId")).agg(
      sum(when($"category" === "AA" && $"transactionDay" <=5, col("transactionAmount")).otherwise(0)).as("AA Total Value"),
      sum(when($"category" === "CC" && $"transactionDay" <=5, col("transactionAmount")).otherwise(0)).as("CC Total Value"),
      sum(when($"category" === "FF" && $"transactionDay" <=5, col("transactionAmount")).otherwise(0)).as("FF Total Value")

    );

    totalTransactionValue.show();

  }*/


  val rddSparkByExample=spark.sparkContext.textFile("/home/binaya/Documents/WiproSerialWiseDocumentationBinaya/test.txt")
  println("Initial number of partitions" + rddSparkByExample.getNumPartitions)
  val repairRDD=rddSparkByExample.repartition(4)
  val flatRDD=rddSparkByExample.flatMap(ll=>ll.split(" "))

  val rdd = sc.parallelize(Seq(List("1","2","3"), List("4", "5", "6")))
  val zz=rdd.flatMap(x=>x)

  val rdd1 = sc.parallelize(Seq(("x", 1.0), ("y", 2.0)))
  val rdd2 = sc.parallelize(Seq(("x", 3.0), ("y", 4.0)))
  val rdd3 = sc.parallelize(Seq(("x", 5.0), ("y", 6.0)))
  val rdds = Array(rdd1, rdd2, rdd3)


}


