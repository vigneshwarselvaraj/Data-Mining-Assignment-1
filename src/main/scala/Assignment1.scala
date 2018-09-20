import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._


//imports to disable info errors
import org.apache.log4j.Logger
import org.apache.log4j.Level
//end of imports

object Assignment1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Assignment1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[2]")
      .appName("Assignment1")
      .getOrCreate

    import spark.implicits._

    val inputFileName = "survey_results_public.csv"
    val outputFileName = "Task1-Output.csv"

    val inputDF = spark.read
                    .format("csv")
                    .option("header", "true")
                    .load(inputFileName)
                    .select("Country", "Salary", "SalaryType")
                    .filter("Salary not in ('NA', '0')").sort("Country")

    inputDF.groupBy("Country").count().show(5)

    val totalRecords = inputDF.count()

    //val countryRDD = inputDF.select("Country").repartition(2).rdd.map(r => r(0))
    val countryRDD = inputDF.select("Country").coalesce(2).rdd.map(r => r(0).toString)
    val interCountryCounts = countryRDD.map(word => (word, 1))


    interCountryCounts.take(1).foreach(println)
    val startTime = System.currentTimeMillis()
    val countryCounts:RDD[(String, Int)] = interCountryCounts.reduceByKey(_+_)//.sortBy(_._1.toString)
    countryCounts.take(5).foreach(println)
    val endTime = System.currentTimeMillis()
    println("First operation - time taken is " + (endTime-startTime))


    val countryDF = countryCounts.toDF("total", totalRecords.toString)
    countryDF.show(10)
    //countryDF.coalesce(1).write.format("csv").option("header", "true").csv(outputFileName)

    //Task 1 Complete. Task 2 code below.

    val numPartitions = countryRDD.getNumPartitions
    println("Number of partitions is " + numPartitions)

    countryRDD.mapPartitionsWithIndex{case (i, rows) => Iterator((i, rows.size))}
      .toDF("Partition Number", "Number of Records").show

    println("Printing countryRDD few records")
    countryRDD.take(5).foreach(println)

    //countryRDD.keyBy(_).partitionBy(partitioner = HashPartition)

    val countryCountRDD = countryRDD.map(word => (word, 1))
    val customPartitionedCountry:RDD[(String, Int)] = countryCountRDD.partitionBy(new HashPartitioner(2))

    customPartitionedCountry.take(1).foreach(println)
    val startTime2 = System.currentTimeMillis()
    val customPartitionCounts = customPartitionedCountry.reduceByKey(_+_)//.sortBy(_._1.toString)
    customPartitionCounts.take(5).foreach(println)
    val endTime2 = System.currentTimeMillis()
    println("Time taken for second operations is " + (endTime2-startTime2))

    val numPartitions2 = customPartitionedCountry.getNumPartitions
    println("Number of partitions is " + numPartitions)

    customPartitionedCountry.mapPartitionsWithIndex{case (i, rows) => Iterator((i, rows.size))}
      .toDF("Partition Number", "Number of Records").show


    //Code for Task 3 below
    /*val inputDF2 = inputDF
                      .withColumn("Salary",  inputDF.col("Salary").cast("Integer"))
        .select("Country", "Salary", "SalaryType")*/

    inputDF.groupBy("Country").agg(count("SalaryType")).show(20)

    val rows1 = inputDF.count()
    val summary1 = inputDF.describe().filter($"summary" === "count")
    summary1.select(inputDF.columns.map(c =>(lit(rows1) - col(c)).alias(c)): _*).show


    val inputDF3 = inputDF.withColumn("Country", inputDF.col("Country"))
                    .withColumn(
                    "Salary",
                    when(inputDF.col("SalaryType") === lit("Monthly"), regexp_replace(inputDF.col("Salary").cast("String"), lit(","), lit("")).cast("Double") * 12)
                  .when(inputDF.col("SalaryType") === lit("Weekly"), regexp_replace(inputDF.col("Salary").cast("String"), lit(","), lit("")).cast("Double") * 52)
                  //.when(inputDF.col("SalaryType") === lit("Yearly"), inputDF.col("Salary").cast("Double") * 1.0)
                  //.when(inputDF.col("SalaryType") === lit("NA"), inputDF.col("Salary").cast("Double") * 1.0)
                        .otherwise(regexp_replace(inputDF.col("Salary").cast("String"), lit(","), lit("")).cast("Double")*  1)
    ).withColumn("SalaryType", inputDF.col("SalaryType") )


    val rows = inputDF3.count()
    val summary = inputDF3.describe().filter($"summary" === "count")
    summary.select(inputDF3.columns.map(c =>(lit(rows) - col(c)).alias(c)): _*).show

    //inputDF3.show(10)
    //inputDF3.groupBy("Country").avg("Salary").show(20)
    val finalDF = inputDF3.groupBy("Country").agg(count("Salary"),
        min("Salary").cast("Integer"),  max("Salary").cast("Integer"), bround(avg("Salary").cast("Double"),2))


    finalDF.show(20)
    val finalOutput = "AggregateValues.csv"
    //finalDF.coalesce(1).write.format("csv").option("header", "true").csv(finalOutput)

  }
}
