package lab.scala.lab8

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.functions.{abs, avg, col, count, max, min, sqrt, sum}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    setActiveSession(spark)

    //Defining Datatype schema for our dataframes
    val schema = new StructType(Array(
      StructField("name", StringType, false),
      StructField("datetime", StringType, false),
      StructField("temp",DoubleType,false),
      StructField("humidity",DoubleType,false),
      StructField("windspeed",DoubleType,false)
    ))
    /*
    * Obtaining DataFrames, selecting our columns and applying schema
    * */
    val valenciaDF = spark.read.format("csv")
      .option("header", "true")
      .load("src/main/resources/datasets/valencia.csv")
        .select(
          col("name").as("name"),
          col("datetime").as("datetime"),
          col("temp").cast(DoubleType).as("temp"),
          col("humidity").cast(DoubleType).as("humidity"),
          col("windspeed").cast(DoubleType).as("windspeed")
        )
    val madridDF = spark.read.format("csv")
      .option("header", "true")
      .load("src/main/resources/datasets/madrid.csv")
        .select(
          col("name").as("name"),
          col("datetime").as("datetime"),
          col("temp").cast(DoubleType).as("temp"),
          col("humidity").cast(DoubleType).as("humidity"),
          col("windspeed").cast(DoubleType).as("windspeed")
    )

    /*Temperature & Humidity month averages
    * We override datetime ignoring individual days in order to group our rows my individual months
    */
    val valenciaMA = valenciaDF.withColumn("datetime", valenciaDF("datetime").substr(0,7))
      .groupBy("datetime")
      .avg("temp","humidity").sort("datetime")
      .sort("datetime")
    val madridMA = madridDF.withColumn("datetime", madridDF("datetime").substr(0,7))
      .groupBy("datetime")
      .avg("temp","humidity")
      .sort("datetime")

    /*Wind averages
     */
    val windAvgValencia = valenciaDF.withColumn("datetime", valenciaDF("datetime").substr(0, 7))
      .groupBy("datetime")
      .agg(
        avg("windspeed").as("windAverage"),
        sqrt(sum("windspeed") / count("windspeed")).as("windVectorAvg")
      ).sort("datetime")
    val windAvgMadrid = madridDF.withColumn("datetime", madridDF("datetime").substr(0, 7))
      .groupBy("datetime")
      .agg(
        avg("windspeed").as("windAverage"),
        sqrt(sum("windspeed") / count("windspeed")).as("windVectorAvg")
      ).sort("datetime")

    /*Monthly min & max temp
    */
    val minMaxValencia = valenciaDF.withColumn("datetime", valenciaDF("datetime").substr(0, 7))
      .groupBy("datetime")
      .agg(
        min("temp").as("tempMin"),
        max("temp").as("tempMax")
      ).sort("datetime")
    val minMaxMadrid = madridDF.withColumn("datetime", madridDF("datetime").substr(0, 7))
      .groupBy("datetime")
      .agg(
        min("temp").as("tempMin"),
        max("temp").as("tempMax")
      ).sort("datetime")

    /*Differences in temperatures between costal and inner land city
    * */
    val reducedMDF = madridDF.select("datetime", "temp")
      .withColumn("mtemp", col("temp"))
      .withColumn("mDatetime", col("datetime"))
    val reducedVDF = valenciaDF.select("datetime", "temp")
      .withColumn("vtemp", col("temp"))
      .withColumn("vDatetime", col("datetime"))

    val joinedDF = reducedVDF.join(reducedMDF , col("vDatetime") === col("mDatetime") , "inner")
      .withColumn("datetime", col("vDatetime").substr(0, 7))
      .groupBy(col("datetime"))
      .agg(abs(col("vtemp") - col("mtemp")).as("Difference") )
  }
}