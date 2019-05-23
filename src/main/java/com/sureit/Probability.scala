package com.sureit
import java.util.Scanner
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import scala.collection.immutable.TreeSet
import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import java.time.{ LocalDate, Period }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrameStatFunctions
import scala.collection.immutable.TreeSet
import org.apache.spark.sql.SQLImplicits
import java.time.{ LocalDate, LocalDateTime, Period, Duration }
import java.time.format.DateTimeFormatter

object Probability {

  def apply(variable: DataFrame) = {
    val spark = getSparkSession()
    val S = new Scanner(System.in)
    import spark.implicits._

    print("Enter Parameter Value : ")
    val Parameter = S.next()
    print("Enter nearer Value : ")
    val nearer = S.next()
    print("Enter distant Value : ")
    val distant = S.next()
    print("Enter dp Value : ")
    val dp = S.next()
    print("Enter prev1 Value : ")
    val prev1 = S.next()
    print("Enter prev2 Value : ")
    val prev2 = S.next()
    print("Enter prev3 Value : ")
    val prev3 = S.next()
    print("Enter prev4 Value : ")
    val prev4 = S.next()
    print("Enter prev5 Value : ")
    val prev5 = S.next()
    print("Enter prev6 Value : ")
    val prev6 = S.next()
    print("Enter prev7 Value : ")
    val prev7 = S.next()
    print("Enter same_state Value : ")
    val same_state = S.next()
    print("Enter daily_pass Value : ")
    val daily_pass = S.next()
    print("Enter monthly_pass Value : ")
    val monthly_pass = S.next()
    print("Enter local Value : ")
    val local = S.next()

    val beta = Array(
      Parameter, nearer, distant, dp, prev1, prev2, prev3, prev4, prev5, prev6, prev7, same_state, daily_pass, monthly_pass, local)
    val toDouble = udf[Double, String](_.toDouble)
    val variableFormatted = variable
      .withColumn("nearer", toDouble($"nearer"))
      .withColumn("distant", toDouble($"distant"))
      .withColumn("dp", toDouble($"dp"))
      .withColumn("prev1", toDouble($"prev1"))
      .withColumn("prev2", toDouble($"prev2"))
      .withColumn("prev3", toDouble($"prev3"))
      .withColumn("prev4", toDouble($"prev4"))
      .withColumn("prev5", toDouble($"prev5"))
      .withColumn("prev6", toDouble($"prev6"))
      .withColumn("prev7", toDouble($"prev7"))
      .withColumn("same_state", toDouble($"same_state"))
      .withColumn("same_state", toDouble($"daily_pass"))
      .withColumn("same_state", toDouble($"monthly_pass"))
      .withColumn("same_state", toDouble($"local"))

    val variableWithZ = variable
      .withColumn(
        "z",
        (lit(beta(0)) +
          lit(beta(1)) * $"nearer" +
          lit(beta(2)) * $"distant" +
          lit(beta(3)) * $"dp" +
          lit(beta(4)) * $"prev1" +
          lit(beta(5)) * $"prev2" +
          lit(beta(6)) * $"prev3" +
          lit(beta(7)) * $"prev4" +
          lit(beta(8)) * $"prev5" +
          lit(beta(9)) * $"prev6" +
          lit(beta(10)) * $"prev7" +
          lit(beta(12)) * $"same_state" +
          lit(beta(13)) * $"daily_pass" +
          lit(beta(13)) * $"monthly_pass" +
          lit(beta(14)) * $"local"))

    print("Enter Cut-off : ")
    val In4 = S.next()
    val variableWithProb = variableWithZ
      .withColumn(
        "prob", bround((lit(1) / (lit(1) + (exp(lit(-1) * $"z")))), 4))

    val variableWithOutcome = variableWithProb.withColumn("event", (when($"prob" > In4, 1).otherwise(0)))
    variableWithOutcome

  }
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      //      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp2")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.7:9000/vivek/temp")
      .getOrCreate()
  }

}