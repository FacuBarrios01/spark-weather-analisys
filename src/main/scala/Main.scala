package lab.scala.lab8

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.functions.{col, month, sqrt, year}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

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

    //Temperature & Humidity month averages
    // We override datetime ignoring individual days in order to group our rows my individual months
    val valenciaMA = valenciaDF.withColumn("datetime", valenciaDF("datetime").substr(0,7))
      .groupBy("datetime")
      .avg("temp","humidity").sort("datetime")
    val madridMA = madridDF.select("datetime","temp")
          .groupBy(valenciaDF("datetime").substr(0,7))
          .avg("temp","humidity")
    //Wind averages
    val windAvgValencia = valenciaDF.withColumn("datetime", valenciaDF("datetime").substr(0, 7))
      .groupBy("datetime").avg("windspeed")
      .withColumn("vectorAvg",
        valenciaDF("windspeed")
//          .agg(
          //Complete from here on
      )
    valenciaMA.printSchema()
    valenciaMA.foreach(print)
    print(valenciaMA)
    print(madridMA)
  }
}