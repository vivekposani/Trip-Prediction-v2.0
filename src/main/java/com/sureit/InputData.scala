package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import java.util.Scanner
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import java.time.{ LocalDate, Period }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.SQLImplicits
import java.time.{ LocalDate, LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import org.apache.spark.SparkException

object InputData extends App {
  case class Record(plaza1: List[String], plaza2: Array[String])
  val t0 = System.currentTimeMillis()

  val spark = getSparkSession()

  val plaza1 = getInputPlaza
  print(plaza1)
  val plaza2 = getInputPlaza
  print(plaza2)

//  val inputVariables = Array(In1, In2)

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }

  def getInputPlaza = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/plaza.txt").map(x => (x(0)))

  }

  def getInputData = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/TagPlazaTimeStateDiscountClassTxn.txt")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6)))

  }

  def writeToCSV(df: DataFrame): Unit = {

    //      print("Enter Variable Creation Version Code : ")
    val Version = 1

    val folder = "hdfs://192.168.70.7:9000/vivek/VariableCreation/" + Version + "/"
    //    val folder2 = "file:///192.168.70.15/Share_Folder/Variable_Creation"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)
    //    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder2)

    val t1 = System.currentTimeMillis()

  }

}