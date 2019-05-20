package com.sureit
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object DaysOfProporion {

  def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]): DataFrame = {

    val spark = getSparkSession()
    import spark.implicits._
    val inputPlaza = inputVariables(0)
    val performanceDate = inputVariables(1)

    val customizedInputData = inputData .filter(x => x._2 == inputPlaza)

      .map(x => (x._1, x._5.substring(0, 10)))
      .distinct()
      .filter(x => (x._2 != performanceDate))
    val tagTimeDF = customizedInputData
      .toDF("tag", "time")
      .withColumn("time", to_date($"time"))
//tagTimeDF.show
    tagTimeDF.createTempView("TagTime")

    val tagCountMinDF = spark.sql("select tag,count(tag) count,min(time) start_date from TagTime group by tag")

    val tagCountDiff = tagCountMinDF.withColumn("diff", datediff(to_date(lit(performanceDate)), $"start_date"))
    val daysProportionDF = tagCountDiff.select($"tag", bround(($"count" / $"diff"), 4) as "dp")

    daysProportionDF

  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
//      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp7")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }
}