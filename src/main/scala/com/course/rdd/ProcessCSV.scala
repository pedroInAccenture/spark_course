package com.course.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProcessCSV extends Serializable{

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark course")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("src/main/resources/data/input/sample.csv")

    rdd.foreach(println)
    //Give it a Structure and select only 4 columns
    case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

    val colsRDD: RDD[SurveyRecord] = rdd.map(line => {
      val cols = line.split(",").map(_.trim)
      SurveyRecord(cols(1).toInt, cols(2), cols(3), cols(4))
    })

    //Apply Filter
    val filteredRDD = colsRDD.filter(r => r.Age < 40)

    //Manually implement the GroupBy
    val kvRDD = filteredRDD.map(r => (r.Country, 1))
    val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)

    colsRDD.foreach(println)

    Thread.sleep(10000000)
  }
}
