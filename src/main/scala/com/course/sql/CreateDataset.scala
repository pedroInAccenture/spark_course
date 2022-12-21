package com.course.sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CreateDataset {
  case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark course")
      .getOrCreate()

    //Read your CSV file
    val rawDF: Dataset[Row] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    rawDF.printSchema()

    import spark.implicits._

    //Type Safe Data Set
    val surveyDS: Dataset[SurveyRecord] = rawDF.select("Age", "Gender", "Country", "state").as[SurveyRecord]
    surveyDS.printSchema()

    //Type safe Filter
    val filteredDS = surveyDS.filter(row => row.Age < 40)

    //Runtime Filter
    val filteredDF = surveyDS.filter("Age  < 40")

    //Type safe GroupBy
    val countDS = filteredDS.groupByKey(r => r.Country).count()

    //Runtime GroupBy
    val countDF = filteredDF.groupBy("Country").count()

    filteredDS.write
      .mode("overwrite")
      .partitionBy("Country")
      .parquet("src/main/resources/data/output/countriesPart.parquet")
  }
}
