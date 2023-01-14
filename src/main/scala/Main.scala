package lab.scala.lab8

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    setActiveSession(spark)

    val valenciaDF = spark.read
      .option("header", "true")
      .format("csv")
      .load("src/main/resources/datasets/valencia.csv")

    valenciaDF.select("name",
      "datetime",
      "temp",
      "humidity",
      "windspeed"
    ).take(5).foreach(println)
  }
}