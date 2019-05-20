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
    import spark.implicits._

    val beta = Array(
      -3.62840, 1.48793, -4.66770, 3.70818, -2.21066, -0.27923, 0.32080, 0.74241, 0.77070, 0.89315, 0.73830, 0.08963)
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
          lit(beta(11)) * $"same_state"))

    val S = new Scanner(System.in)
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