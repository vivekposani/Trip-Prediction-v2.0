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

object VariableDynamic extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val t0 = System.currentTimeMillis()
  //  val In2 = "2018-10-01"
  val spark = getSparkSession()
//  val In = getInputPlaza.mapPartitionsWithIndex {
//    (idx, iter) => if (idx == 0) iter.drop(1) else iter
//  }
  val plazalist = getInputPlaza.collect.toList.par
  val inputData = getInputData.persist(StorageLevel.DISK_ONLY)

  //   val plaza = "8001"
  //  for (plaza <- plazalist if plaza != null if i < 2) {
  //    print("Started running for Plaza : " + plaza)
  //    val inputVariables = Array(plaza, In2)
  //    val Variable = VariableCreation(inputData, inputVariables)
  //    println("count of rows :"+ Variable.count())
  //    writeToCSV(Variable, plaza)
  //    print("Plaza " + plaza + " Done")

  //  }

  //  val plazaWithBeta = getInputPlaza

  plazalist.map { x =>

    val plazaWithBetaArray = x.split(";")
    val plaza = plazaWithBetaArray(0)
    val date = plazaWithBetaArray(1)
//    val vclass = plazaWithBetaArray(2)
    //    val beta = plazaWithBetaArray(1).split(",")
    println("Started running for Plaza : " + plaza)
    val inputVariables = Array(plaza, date)
    val lastplaza = LastPlaza(inputData, inputVariables)
    val variables = VariableCreation(inputData, inputVariables)
    .join(lastplaza, Seq("tag"), "left_outer")
//    variables.show(10)

    writeToCSV(variables, plaza, date)
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

  def writeToCSV(df: DataFrame, plaza: String, date: String): Unit = {

    //      print("Enter Variable Creation Version Code : ")

    val folder = "hdfs://192.168.70.7:9000/vivek/VariableCreation/" + plaza + "/" + date + "/"
    //    val folder2 = "file:///192.168.70.15/Share_Folder/Variable_Creation"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)
    //    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder2)

    val t1 = System.currentTimeMillis()
    print((t1 - t0).toFloat / 1000)

  }

}