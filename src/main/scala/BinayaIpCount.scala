import java.util

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}


case class Record(id: Int, end_date: String, start_date: String, location: String)

object BinayaIpCount {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf
      .setMaster("local[*]")
      .setAppName("SparkTestJob")

println("allright")

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .config(conf)
      .getOrCreate()
import spark.implicits._
    val fundamentals_raw = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("/home/binaya/Desktop/Optum/nyse/fundamentals.csv")
    val tt=fundamentals_raw.columns.foreach(println(_))

    import spark.implicits._

    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
    val dataa = spark.sparkContext.parallelize(keysWithValuesList).collect().foreach(println)

    //Create key value pairs
    /*val kv = dataa.map(_.split("=")).map(v => (v(0), v(1))).cache()
    val initialCount = 0;
    val addToCounts = (n: Int, s: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2
    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
*/

    val list = spark.sparkContext.parallelize(List(1,2,3,4),3)
    val list2 = list.glom().collect
    val res12 = list.aggregate((1,0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    println(res12)

    val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd=spark.sparkContext.parallelize(dataSeq)

    val rdd3 = spark.sparkContext.wholeTextFiles("/home/binaya/Desktop/Optum/test.txt").keys.collect()
    println(rdd3.mkString(" "))

    val rdd1= spark.sparkContext.textFile("/home/binaya/Desktop/Optum/test.txt")
    val rdd2 = rdd1.flatMap(f=>f.split(" ")).filter(_.startsWith("a"))



    val rdd4 = spark.sparkContext.wholeTextFiles("/home/binaya/Desktop/Optum/test.txt").values.collect()
   println(rdd4.mkString(" "))

    val rdd6= rdd2.map(m=>(m,1)).reduceByKey(_+_)
    val rdd7 =rdd6.map(a=>(a._2,a._1)).sortByKey()
    println(rdd7.count())

    val firstRec = rdd7.first()
    println("First Record : "+firstRec._1 + ","+ firstRec._2)

    println("hello Binaya")

    val datMax = rdd7.max()
    println("Max Record : "+datMax._1 + ","+ datMax._2)

    val totalWordCount = rdd7.reduce((a,b) => (a._1+b._1,a._2))
    println("dataReduce Record : "+totalWordCount._1+" "+ totalWordCount._2)

    //val rdd:RDD[String] = spark.sparkContext.textFile("src/main/scala/test.txt")
    /*val fundamentals_df=fundamentals_raw.toDF(fundamentals_raw.columns.map(x=>x.trim.toLowerCase()):_*).withColumn("symbol", col("ticker symbol").substr(0, 2))
      .groupBy("symbol").agg(sum("accounts payable") as "accounts payable",
      sum("accounts receivable") as "accounts receivable")

    val saveTo: String = "/home/binaya/Desktop/Optum/nysee/"

    fundamentals_df
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(saveTo)

    val prices=spark.read.format("com.databricks.spark.csv").option("header","true")
      .load("""/home/binaya/Desktop/Optum/nyse/prices.csv""").withColumn("symbol",col ("symbol").substr(0,2)).cache()

    val pricesDFwrite=prices.repartition(4).write.format("com.databricks.spark.csv")
      .option("header","true").mode(SaveMode.Overwrite).save(saveTo)

    val real_skew = //|2010-01-25|    AB| 26.790001|54.699952|     26.68| 55.50995|1.53944E7|
      (1 to 1e6.toInt).map(x => ("2010-01-25","AB",1.2,9.8,4.5,78.9,1235543))
        .toDF("date","symbol","f1","f2","f3","f4","f5")

    prices.withColumnRenamed("open","f1").withColumnRenamed("close","f2")
      .withColumnRenamed("low","f3").withColumnRenamed("high","f4")
    .withColumnRenamed("volume","f5").union(real_skew)/*.union(real_skew).union(real_skew)*/
      .write.format("com.databricks.spark.csv").option("header","true").partitionBy("symbol")
    .mode(SaveMode.Overwrite).save(saveTo)

    val anotherLargeDataSet=prices.groupBy("date","symbol").agg(min("open"),
      max("close"),min("low"),max("high"),sum("volume")).cache()

    val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

    val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

    val data=listRdd.collect()
    data.foreach(println)
    def param0= (accu:Int, v:Int) => accu + v
    def param1= (accu1:Int,accu2:Int) => accu1 + accu2
    println("aggregate : "+listRdd.aggregate(0)(param0,param1))

    val real_agg_skew =
      (1 to 1e6.toInt).map(x => ("2010-01-25", "AA", 1.2, 9.8, 4.5, 78.9, 1235543))
    val real_agg_skew_df = spark.sparkContext.parallelize(real_agg_skew).toDF("date", "symbol", "open", "close", "low", "high", "volume")
*/
    /*val rdd = spark.sparkContext.textFile("/home/binaya/Desktop/Optum/test.txt")
    println(rdd.getNumPartitions)
    val reparRdd = rdd.repartition(4)
    println("re-partition count:"+reparRdd.getNumPartitions)

    rdd.collect().foreach(println)

    // rdd flatMap transformation
    val rdd2 = rdd.flatMap(f=>f.split(" ")).map(l => l(1))
    rdd2.foreach(f=>println(f))
    val rdd3=rdd.map(f=>f.split(" ").mkString("/"))
    rdd3.foreach(f=>println(f))*/

//Adding new coulmns and replacing columns
  /*  val data = Seq(Row(Row("James ", "", "Smith"), "36636", "M", "3000"),
      Row(Row("Michael ", "Rose", ""), "40288", "M", "4000"),
      Row(Row("Robert ", "", "Williams"), "42114", "M", "4000"),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", "4000"),
      Row(Row("Jen", "Mary", "Brown"), "", "F", "-1")
    )
    val schema = new StructType()
      .add("name", new StructType()
      .add("firstName", StringType)
      .add("MiddleName", StringType)
      .add("lastname",StringType))

      .add("dob", StringType)
      .add("gender", StringType)
      .add("salary", StringType)

    val dff = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    dff.withColumn("salary", col("salary").cast("Integer")).show(false)
    dff.withColumn("salaryy",col("salary")*100).show(false)
   dff.withColumn("CopiedColumn",col("salary")* -1).show(false)
    dff.withColumn("Country", lit("USA")).show(false)

    //Dataframe Join oprations

    val emp = Seq(Row("1","Smith","-1","2018","10","M","3000"),
      Row("2","Rose","1","2010","20","M","4000"),
      Row("3","Williams","1","2010","10","M","1000"),
      Row("4","Jones","2","2005","10","F","2000"),
      Row("5","Brown","2","2010","40","","-1"),
      Row("6","Brown","2","2010","50","","-1")
    )



    /*val empColumns = Seq(Row("emp_id","name","superior_emp_id","year_joined",
      "emp_dept_id","gender","salary"))*/
val empSchema=new StructType().add("emp_id",StringType).add("name",StringType).add("superior_emp_id",StringType)
  .add("year_joined",StringType).add("emp_dept_id",StringType).add("gender",StringType)
  .add("salary",StringType)
    import spark.sqlContext.implicits._


    val empDF=spark.createDataFrame(spark.sparkContext.parallelize(emp),empSchema)
    empDF.registerTempTable("EMP")

    val dept = Seq(Row("Finance",10),
     Row("Marketing",20),
     Row("Sales",30),
     Row("IT",40)
    )

    val deptRdd=spark.sparkContext.parallelize(dept)


    val deptColumns = Seq("dept_name","dept_id")
    val deptStructType=StructType(List(StructField("dept_name",StringType),StructField("dept_id",IntegerType,true)))
    val deptDF = spark.createDataFrame(deptRdd,deptStructType)
    deptDF.show(false)
    deptDF.registerTempTable("Deptt")

    val addressTxt=Seq((1,"1523 Main St","SFO","CA"),
      (2,"3453 Orange St","SFO","NY"),
      (3,"34 Warner St","Jersey","NJ"),
      (4,"221 Cavalier St","Newark","DE"),
      (5,"789 Walnut St","Sandiago","CA")
    )

    val addressColumns=Seq("emp_id","addline1","city","state")
    val addressDF=addressTxt.toDF(addressColumns:_*)
    addressDF.registerTempTable("AddressTabl")

    //3 tables Join

    /*val resultantData=empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"inner").
      join(addressDF,empDF("emp_id")===addressDF("emp_id"),"inner").show(false)

    println("------------->")

    val resultantDataa=empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"right").
      join(addressDF,empDF("emp_id")===addressDF("emp_id"),"right").show(false)*/
  /* println("========>")
  spark.sql("select * from EMP e,Deptt d,AddressTabl a where e.emp_dept_id==d.dept_id AND e.emp_id==a.emp_id").show(false)
    spark.sql("select * from EMP e INNER JOIN Deptt d ON e.emp_dept_id==d.dept_id INNER JOIN AddressTabl a ON e.emp_id==a.emp_id").show(false)
*/


    //Another 3 table join

    val Student=Seq((1,"ali"),(2,"ahmed"),(3,"john"),(4,"king"))
    val studentSchema=Seq("id","name")
    val studentDF=Student.toDF(studentSchema:_*)

    val Course=Seq((1,"Physics"),(2,"maths"),(3,"computer"),(4,"chemistry"))
    val courseSchema=Seq("id","name")
    val courseDF=Course.toDF(courseSchema:_*)

    val Bridge=Seq((1,1),(1,2),(1,3),(1,4),(2,1),(2,2),(3,3),(3,4),(4,1),(4,2))
    val bridgeSchema=Seq("sid","cid")
    val bridgeDF=Bridge.toDF(bridgeSchema:_*)*/
//
//    val resultDF=studentDF.join(bridgeDF,studentDF("id")===bridgeDF("sid"),"inner")
//      .join(courseDF,bridgeDF("cid")===courseDF("id"),"inner").select(studentDF("name"),courseDF("name")).show(false)
//
//
//    //2 table join
//    val Employees=Seq((1,"ali"),(2,"king"),(3,"mak"),(4,"sam"),(5,"jon"))
//    val employeesSchem=Seq("id","name")
//    val employeesDF=Employees.toDF(employeesSchem:_*)
//    employeesDF.registerTempTable("employee")
//
//    val Manager=Seq((1,2),(1,3),(3,4),(4,5))
//    val managerSchema=Seq("mid","eid")
//    val managerDF=Manager.toDF(managerSchema:_*)
//    managerDF.registerTempTable("manage")
//
//    spark.sql("select e1.name , e2.name  from employee e1 left join manage m on m.mid == e1.id left join employee e2 on m.eid == e2.id").show(false)
//
//
//    //Another 3 table join
//

    //val agentss=Seq(("A007","Ramasundar","Bangalore","0.15","077-25814763"),("A003","Alex","London","0.13","075-12458969"),("A008","Alford","New York","0.12","044-25874365"),("A011","Ravi Kumar","Bangalore","0.15","077-45625874"),("A010","Santakumar","Chennai","0.14","007-22388644"),("A012","Lucida","San Jose","0.12","044-52981425"),("A005","Anderson","Brisban","0.13","045-21447739"),("A001","Subbarao","Bangalore","0.14","077-12346674"),("A002","Mukesh","Mumbai","0.11","029-12358964"),("A006","McDen","London","0.15","078-22255588"),("A004","IvanTorento","0.15","008-22544166"))
   val agents=Seq(Row("A007","Ramasundar","Bangalore",0.15,"077-25814763"),Row("A003","Alex","London",0.13,"075-12458969"),Row("A008","Alford","New York",0.12,"044-25874365"),Row("A011","Ravi Kumar","Bangalore",0.15,"077-45625874"),Row("A010","Santakumar","Chennai",0.14,"007-22388644"),Row("A012","Lucida","San Jose",0.12,"044-52981425"),Row("A005","Anderson","Brisban",0.13,"045-21447739"),Row("A001","Subbarao","Bangalore",0.14,"077-12346674"),Row("A002","Mukesh","Mumbai",0.11,"029-12358964"),Row("A006","McDen","London",0.15,"078-22255588"),Row("A004","Ivan","Torento",0.15,"008-22544166"),Row("A009","Benjamin","Hampshair",0.11,"008-22536178"))

  //val agentsColumns=Seq("AGENT_CODE","AGENT_NAME","WORKING_AREA","COMMISSION","PHONE_NO")
    val agentsColumns=StructType(Seq(StructField("AGENT_CODE",StringType,true),StructField("AGENT_NAME",StringType,true),
      StructField("WORKING_AREA",StringType,true),StructField("COMMISSION",DoubleType,true),
      StructField("PHONE_NO",StringType,true)))

    val agentsDF=spark.createDataFrame(spark.sparkContext.parallelize(agents),agentsColumns)
    agentsDF.printSchema()
    agentsDF.registerTempTable("agents")


    //val customerSchema=Seq("CUST_CODE","CUST_NAME","CUST_CITY","WORKING_AREA","CUST_COUNTRY","GRADE","OPENING_AMT","RECEIVE_AMT","PAYMENT_AMT","OUTSTANDING_AMT","PHONE_NO","AGENT_CODE","BlankColumn")
   val customerSchema=StructType(List(StructField("CUST_CODE",StringType,true),StructField("CUST_NAME",StringType,true),
     StructField("CUST_CITY",StringType,true),StructField("WORKING_AREA",StringType,true),StructField("CUST_COUNTRY",StringType,true),
     StructField("GRADE",IntegerType,true),StructField("OPENING_AMT",DoubleType,true),StructField("RECEIVE_AMT",DoubleType,true),
     StructField("PAYMENT_AMT",DoubleType,true),StructField("OUTSTANDING_AMT",DoubleType,true),
     StructField("PHONE_NO",StringType,true),StructField("AGENT_CODE",StringType,true)))
    val customerDataf=spark.read.format("com.databricks.spark.csv").option("delimiter", "|").schema(customerSchema).load("/home/binaya/Desktop/Optum/customerr.txt")
    //customerDataf.printSchema()*/
    /*customerDataf.printSchema()*/
    /*customerDataf.registerTempTable("customer")

    val orderDF = spark.sparkContext.textFile("/home/binaya/Desktop/Optum/orders.txt")
    var newRDD = orderDF.map(elt => elt.replaceAll("""[\t\p{Zs}]+""", " "))
    val newOrderDF=newRDD.map(x => x.split(' ')).map(v=>(v(0).trim.toInt,v(1).trim.toInt,v(2).trim.toDouble,v(3).trim.toString,v(4).trim.toString,v(5).trim.toString)).toDF("ORD_NUM","ORD_AMOUNT","ADVANCE_AMOUNT","ORD_DATE","CUST_CODEE","AGENT_CODEE")
    newOrderDF.printSchema()
    newOrderDF.registerTempTable("orders")*/
 //val resl=spark.sql("select * from orders cross join customer cross join agents").count()
   // println(resl)

   /*val resultantDF=newOrderDF.join(customerDataf, newOrderDF("CUST_CODEE")===customerDataf("CUST_CODE"),"inner")
      /*.join(agentsDF,newOrderDF("AGENT_CODEE")===agentsDF("AGENT_CODE"),"inner"*/) /*/*&& customerDataf("CUST_CITY")===agentsDF("WORKING_AREA")*/*/
      .select("ORD_NUM","CUST_CODE","CUST_NAME","CUST_CITY","AGENT_CODEE").show(false)
*/
   /*val resultantDF=newOrderDF.join(customerDataf, newOrderDF("CUST_CODEE")===customerDataf("CUST_CODE"),"inner")
     .select("ORD_NUM","CUST_CODE","CUST_NAME","CUST_CITY","AGENT_CODEE").show(false)
*/

    /*spark.sql("select a.ORD_NUM,a.CUST_CODE,b.CUST_NAME," +
      "b.CUST_CITY,c.AGENT_CODE from orders a,customer b,agents c where b.CUST_CITY=c.WORKING_AREA AND " +
      "a.CUST_CODE=b.CUST_CODE AND a.AGENT_CODE=c.AGENT_CODE").show(false)
*/
    /*spark.sql("select a.ORD_NUM,b.CUST_CODE,b.CUST_NAME," +
      "b.CUST_CITY,c.AGENT_CODE from orders a,customer b,agents c where b.CUST_CITY==c.WORKING_AREA").show(false)
//List list=new util.ArrayList[]()*/



    //println(newOrderRDD.mkString("/n"))
    //val df = orderDF.map(x => x.trim(" ")).map(x => ((x(0)).toInt, x(1), x(2), x(3),x(4),x(5))).toDF("ORD_NUM","ORD_AMOUNT","ADVANCE_AMOUNT","ORD_DATE","CUST_CODE","AGENT_CODE").select("ORD_NUM","CUST_CODE").show(false)
    //val ordersSchema=Seq("ORD_NUM","ORD_AMOUNT","ADVANCE_AMOUNT","ORD_DATE","CUST_CODE","AGENT_CODE")
   //val orderDF=spark.read.format("csv").option("sep"," ".trim).load("/home/binaya/Desktop/Optum/orders.csv").toDF(ordersSchema:_*)
   ///val oredrsDF=spark.read.text("/home/binaya/Desktop/Optum/orders.txt").toDF(ordersSchema:_*)
    //df.printSchema()



   /* val dataset1=Seq(("1",null,null,null,null),("2","A2","A21","A31","A41"))
    val df1=dataset1.toDF("id","val1","val2","val3","val4")

    val dataset2=Seq(("1","B1","B21","B31","B41"),("2",null,null,null,null))
    val df2=dataset2.toDF("id1","val1","val2","val3","val4")

    val dataset3=Seq((1,"C1","C2","C3","C4"),(2,"C11","C12","C13","C14"))
    val df3=dataset3.toDF("id","val1","val2","val3","val4")

    val resultdata=df1.join(df2,df1("id")===df2("id1")).select(col("id"),coalesce(df1("val1"),df2("val1")).as("finalValue1"),
      coalesce(df1("val2"),df2("val2")).as("finalVal2"),coalesce(df1("val3"),df2("val3")).as("finalVal3"),
      coalesce(df1("val4"),df2("val4")).as("finalVal4")).show(false)*/
//https://www.sqlshack.com/learn-sql-join-multiple-tables/
    //https://www.dofactory.com/sql/join

    /*spark.sql("select * from EMP e inner join Deptt d ON e.emp_dept_id==d.dept_id").show(false)
    spark.sql("select * from EMP e left outer join Deptt d ON e.emp_dept_id==d.dept_id").show(false)
    val qqw=spark.sql("select * from EMP e cross join Deptt d").count()
    println(qqw)*/
//https://sparkbyexamples.com/spark/spark-sql-dataframe-join/
    /*empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"inner").show(false)
    empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"leftouter").show(false)
    empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"right").show(false)
    empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"fullouter").show(false)*/
    /*empDF.join(deptDF,empDF("emp_dept_id")===deptDF("dept_id"),"leftsemi").show(false)

empDF.as("emp1").join((empDF).as("emp2"),col("emp1.emp_id")===col("emp2.superior_emp_id"),"inner").select(col("emp1.emp_id"),col("emp2.emp_id").as("superior_emp_id")
  ,col("emp1.name"),col("emp2.name").as("supervisorname")).show(false)*/


    /* val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val myFile = spark.sparkContext.textFile("/home/binaya/dataFrameCountry.txt")
    val df = myFile.map(x => x.split('|')).map(x => Record(x(0).toInt, x(1), x(2), x(3))).toDF()
    df.show()
    df.registerTempTable("employee")
    val flattenDF = df.select($"id", $"end_date", $"start_date", expr("(split(location, '-'))[0]").cast("string").as("state"), expr("(split(location, '-'))[1]").cast("string").as("city")).show()
    val results = spark.sql(" select id ,DATEDIFF(end_date,start_date) as diff_days from employee").show()

    val dfWithSchema = df.withColumn("date_diff", dateDiff(df("start_date"), df("end_date")))
     dfWithSchema.show()
  }
  
      val dateDiff = udf( (start_date: String,end_date:String) => {

      val start_date_only=start_date.split(" ")(0)
      val year1=start_date_only.split("-")(0)
      val month1=start_date_only.split("-")(1)

      val day1=start_date_only.split("-")(2)

      import java.time.LocalDate;

      val date1 = LocalDate.of(year1.toInt, month1.toInt, day1.toInt);

      val end_date_only=end_date.split(" ")(0)
      val year2=end_date_only.split("-")(0)
      val month2=end_date_only.split("-")(1)

      val day2=end_date_only.split("-")(2)

        import java.time.LocalDate;

   val date2 = LocalDate.of(year2.toInt, month2.toInt, day2.toInt);

        import java.time.Period
        val period = Period.between(date2, date1)
          period.getDays})*/
  }
}
    
