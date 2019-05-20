package com.sureit
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark._
object DistinctTag {

  def apply(inputData: RDD[(String, String, String, String, String, String, String)], inputVariables: Array[String]) = {
    val spark = getSparkSession()
    import spark.implicits._
    val performanceDate = inputVariables(1)
    val inputPlaza = inputVariables(0)
    val distinctTag = inputData
      .filter(x => x._5.substring(0, 10) != performanceDate)
      .filter(x => (x._2 == inputPlaza))
      .map(x => (x._1))
      .distinct
    val distinctDF = distinctTag.toDF("tag")
    distinctDF
  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      //      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp5")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")

      .getOrCreate()
  }
}