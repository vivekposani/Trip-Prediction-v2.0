package com.sureit
import org.apache.spark._
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

object VariableCreation extends App {
  //    Logger.getLogger("org").setLevel(Level.ERROR)

  val S = new Scanner(System.in)
  print("Enter Plaza Code : ")
  val In1 = S.next()
  print("Enter performanceDate Code : ")
  val In2 = S.next()
  print("Enter MaxDate Code : ")
  val In3 = S.next()
  val spark = getSparkSession()

  val t0 = System.currentTimeMillis()
  println("Running Variable Creation")
  val inputVariables = //Array("54002", "2017-06-19", "2017-04-01")
    Array(In1, In2, In3)
  val inputPlaza = inputVariables(0)
  val performanceDate = inputVariables(1)
  val inputDate = LocalDate.parse(inputVariables(1))
  val startDate = inputVariables(2)

  val inputData = getInputData
  // .filter(x => x._1 == "9189070480100001CD5C")
  //  .persist()

  val inputDataFiltered = inputData
    .filter(x => (x._5.substring(0, 10) <= performanceDate))
    .filter(x => (x._5.substring(0, 10) >= startDate))
    .persist

  val daysOfProportion = DaysOfProporion(inputDataFiltered, inputVariables)
  val prev1to7 = Prev1to7(inputDataFiltered, inputVariables)
  val txnOnPerformanceDate = TxnOnPerformanceDate(inputDataFiltered, inputVariables)
  val distanceFromPreviousTxn = DistanceFromPreviousTxn(inputDataFiltered, inputVariables)

  val variables = txnOnPerformanceDate
    .join(distanceFromPreviousTxn, Seq("tag"), "outer")
    .join(daysOfProportion, Seq("tag"), "outer")
    .join(prev1to7, Seq("tag"), "outer")

  val distinctTag = DistinctTag(inputDataFiltered, inputVariables)

  val variable = distinctTag.join(variables, Seq("tag"), "left_outer")
    .na.fill(0)
  //  println(variables.count)
  writeToCSV(variable, "variable.csv")
  //  variable.write.mode(SaveMode.Overwrite).csv("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV")

  val t1 = System.currentTimeMillis()
  println((t1 - t0).toFloat / 1000)

/******************************************************/

  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      //      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp6")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }

  def getInputData = {

    val spark = getSparkSession()
    //spark.sparkContext.textFile("file:///D:/task/data/TagPlazaCodeEntryExitLaneTime.txt")
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/TagPlazaEnExLaneTimeStateDiscount.txt")

      .map(_.split(","))

      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6)))

  }
  def writeToCSV(df: DataFrame, file: String): Unit = {
    val folder = "hdfs://192.168.70.7:9000/vivek/VariableCreation/2.3/04"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder + file)
  }

  //9189070480100001CD5C 45 104

}
