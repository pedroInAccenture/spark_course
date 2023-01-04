package com.course

import com.course.common.Operation
import org.apache.spark.sql.SparkSession

object ExecuteApp {
  def main(args: Array[String]): Unit = {

    println(">>>>> Starting App...")
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark course")
      .getOrCreate()

    //Read your CSV file

//    val df = spark.read
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .csv("C:\\Users\\pedro.a.ortega\\Documents\\spark_examples\\spark_course\\src\\main\\resources\\data\\input\\salaries.csv")

    println(">>>>> Reading data...")
    val df = spark.read
    .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(args(0))

    println(">>>>> Transforming data...")
    val dfWithCol = Operation.addColumnRowNumber(df)


    val isNotEmpty = !dfWithCol.isEmpty
    println(s">>>>>> ${isNotEmpty}")

    if(isNotEmpty){
      println(">>>>> Saving data in: "+ args(1))
      dfWithCol.write
        .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
        .mode("overwrite")
        .save(args(1))
    }
    spark.stop()
  }
}
