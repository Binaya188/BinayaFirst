import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object Customer_Sale_Info {

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


    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val cust_info_rdd = spark.sparkContext.textFile("/home/binaya/customer_info.txt")
    val purchase_info_rdd = spark.sparkContext.textFile("/home/binaya/purchase_info.txt")

    val cust_info_df = cust_info_rdd.map(x => x.split('|')).map(x => (x(0).toInt, x(1), x(2), x(3))).toDF("customer_id", "email_id", "language", "location")
    //cust_info_df.show()
    cust_info_df.registerTempTable("customer_info")

    val purchase_info_df = purchase_info_rdd.map(x => x.split('|')).map(x => (x(0).toInt, x(1).toInt, x(2).toInt, x(3).toDouble, x(4))).toDF("transaction_id", "product_id", "customer_id", "sell_price", "item_description")
    //purchase_info_df.show()
    purchase_info_df.registerTempTable("purchase_info")

   /* val loc_max_prodctId=spark.sql ( """select location, product_id
      from
      ( select test.location,test.product_id, RANK() OVER (PARTITION BY test.location,test.product_id ORDER BY test.total_sell DESC) AS sell_rnk
      from (
        select ci.location,pi.product_id,sum(pi.sell_price) as total_sell
      from customer_info ci, purchase_info pi
      where ci.customer_id= pi.customer_id
    group by ci.location, pi.product_id ) test )
    where sell_rnk = 1""")

    loc_max_prodctId.show()
*/


    /*val cust_max_item=spark.sql(
      """
        |select test1.email_id, RANK() OVER (PARTITION BY test.customer_id,test.total_item ORDER BY test.total_item DESC) AS cust_rnk
        |        from (
        |        select ci.customer_id, count(pi.product_id) as total_item
        |        from customer_info ci, purchase_info pi
        |        where ci.customer_id = pi.customer_id
        |        group by ci.customer_id ) test ,  customer_info test1
        |        where test1.customer_id = test.customer_id
      """.stripMargin)

    cust_max_item.show()

    val cust_max_money=spark.sql(
      """
        |select test1.email_id, RANK() OVER (PARTITION BY test.customer_id ORDER BY test.total_purchase DESC) AS cust_rnk
        |from (
        | select ci.customer_id, sum(pi.sell_price) total_purchase
        | from customer_info ci, purchase_info pi
        | where ci.customer_id = pi.customer_id
        | group by ci.customer_id ) test ,  customer_info test1
        | where test1.customer_id = test.customer_id
      """.stripMargin)
    cust_max_money.show()*/

    val prod_min_sale_interms_of_money=spark.sql(
      """
        |select test2.item_description from (
        |select test.product_id, RANK() OVER (PARTITION BY test.product_id ORDER BY test.total_sell asc) AS prd_rnk
        |from (
        | select pi.product_id,sum(pi.sell_price) total_sell
        | from customer_info ci, purchase_info pi
        | where ci.customer_id = pi.customer_id
        | group by pi.product_id ) test ) test1, purchase_info test2
        | where test1.product_id = test2.product_id
        | and test1.prd_rnk = 1
      """.stripMargin)
    prod_min_sale_interms_of_money.show()

    val prod_min_sale_interms_of_numberof_Units=spark.sql(
      """
        |select test2.item_description from (
        |select test.product_id, RANK() OVER (PARTITION BY test.product_id ORDER BY test.total_sell asc) AS prd_rnk
        |from (
        | select pi.product_id,count(pi.product_id) total_sell
        | from customer_info ci, purchase_info pi
        | where ci.customer_id = pi.customer_id
        | group by pi.product_id ) test ) test1, purchase_info test2
        | where test1.product_id = test2.product_id
        | and test1.prd_rnk = 1
      """.stripMargin)

    prod_min_sale_interms_of_numberof_Units.show()
  }
}