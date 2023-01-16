import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.functions.{abs, avg, col, count, first, max, min, month, round, sqrt, sum, year}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Main {
  def main(args: Array[String]): Unit = {
    val OUTPUT_PATH = "/home/facu/IdeaProjects/lab8-sbt/src/main/resources/output"
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

    valenciaDF.write.option("header",true)
      .mode("overwrite")
      .csv(OUTPUT_PATH + "/valenciaDF")
    madridDF.write.option("header",true)
      .mode("overwrite")
      .csv(OUTPUT_PATH + "/madridDF")

    /*Temperature & Humidity month averages
    * We override datetime ignoring individual days in order to group our rows my individual months
    */
    val valenciaMA = valenciaDF.withColumn("datetime", valenciaDF("datetime").substr(0,7))
      .groupBy("datetime")
      .avg("temp","humidity").sort("datetime")
      .sort("datetime")
      .toDF()
    val madridMA = madridDF.withColumn("datetime", madridDF("datetime").substr(0,7))
      .groupBy("datetime")
      .avg("temp","humidity")
      .sort("datetime")
      .toDF()

    valenciaMA.write.option("header",true).mode("overwrite").csv(OUTPUT_PATH + "/valenciaMA")
    madridMA.write.option("header",true).mode("overwrite").csv(OUTPUT_PATH + "/madridMA")
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
    windAvgMadrid.write.option("header",true).mode("overwrite").csv(OUTPUT_PATH + "/madridWAV")
    windAvgValencia.write.option("header",true).mode("overwrite").csv(OUTPUT_PATH + "/valenciaWAV")

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
    minMaxMadrid.write.option("header",true).mode("overwrite").csv(OUTPUT_PATH + "/madridMinMaxT")
    minMaxValencia.write.option("header",true).mode("overwrite").csv(OUTPUT_PATH + "/valenciaMinMaxT")

    /*Differences in temperatures between costal and inner land city
    * */
    val reducedMDF = madridDF.select("datetime", "temp")
      .withColumn("mtemp", col("temp"))
      .withColumn("mDatetime", col("datetime"))
      .drop("datetime","temp")
    val reducedVDF = valenciaDF.select("datetime", "temp")
      .withColumn("vtemp", col("temp"))
      .withColumn("vDatetime", col("datetime"))
      .drop("datetime", "temp")

    val diffTempsDF = reducedVDF.join(reducedMDF , col("vDatetime") === col("mDatetime") , "right")
      .withColumn("datetime", col("vDatetime").substr(0, 7))
      .drop("vDatetime", "mDatetime")
      .withColumn("difference",
        round(abs(reducedMDF.col("mtemp") - reducedVDF.col("vtemp")))
      )

    val monthlyMaxDiff = diffTempsDF.groupBy("datetime").max("difference")
    monthlyMaxDiff.write.option("header",true).mode("overwrite").csv(OUTPUT_PATH + "/monthlyMaxDif")
  }
}