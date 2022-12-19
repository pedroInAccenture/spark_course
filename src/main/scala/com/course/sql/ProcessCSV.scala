package com.course.sql

import org.apache.spark.sql.SparkSession

object ProcessCSV {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark course")
      .getOrCreate()

    val dfData = spark.read.csv("src/main/resources/data/input/sample.csv")
    dfData.show()
  }
}
