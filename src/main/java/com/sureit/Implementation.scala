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
import scala.Array
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

object Implementation extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val t0 = System.currentTimeMillis()
  val In2 = "2018-10-01"
  val spark = getSparkSession()
  val plazalist = getInputPlaza.collect().toList
  val inputData = getInputData.persist(StorageLevel.DISK_ONLY)
  //  var i = 0
  //   val plaza = "8001"
  //  for (plaza <- plazalist if plaza != null if i < 2) {
  //    print("Started running for Plaza : " + plaza)
  //    val inputVariables = Array(plaza, In2)
  //    val Variable = VariableCreation(inputData, inputVariables)
  //    println("count of rows :"+ Variable.count())
  //    writeToCSV(Variable, plaza)
  //    print("Plaza " + plaza + " Done")

  //    i += 1
  //  }

  //  val plazaWithBeta = getInputPlaza

  plazalist.map { x =>
    //    print("begin")
    val plazaWithBetaArray = x.split(";")
    val plaza = plazaWithBetaArray(0)
    val beta = plazaWithBetaArray(1).split(",")
    println("Started running for Plaza : " + plaza)
    val inputVariables = Array(plaza, In2)
    val variables = VariableCreation(inputData, inputVariables)
    val implementationOut = Probability(variables, beta)
    //    val out = implementationOut.collect().map(x => (x(0), x(1), x(17)))

    writeToCSV(implementationOut, plaza)
    println("Plaza " + plaza + " Done")

  }

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
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/Plaza.txt")

  }

  def getInputData = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/TagPlazaTimeStateDiscountClassTxn.txt")
      .map(_.split(","))
      //    val y = (t.filter(x => x.size != 7))
      //    println(y.count)
      //    val z = y.map(x => x.mkString(","))
      //    z.take(10).foreach(println)
      .map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6)))

  }

  def writeToCSV(df: DataFrame, plaza: String): Unit = {

    //      print("Enter Variable Creation Version Code : ")

    val folder = "hdfs://192.168.70.7:9000/vivek/VariableCreation/" + plaza + "/"
    //    val folder2 = "file:///192.168.70.15/Share_Folder/Variable_Creation"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)
    //    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder2)

    val t1 = System.currentTimeMillis()
    print((t1 - t0).toFloat / 1000)

  }

}