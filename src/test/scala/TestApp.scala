import com.course.common.Operation
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.util

class TestApp extends AnyFunSuite {


  test("test if the dataframe is not empty") {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark course")
      .getOrCreate()

    //Read your CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/input/salaries.csv")


    val dfWithCol = Operation.addColumnRowNumber(df)


    val isNotEmpty = !dfWithCol.isEmpty
    print(s">>>>>> ${isNotEmpty}")

    assert(isNotEmpty)
  }

  test("multiply one by any other number is second one") {
//    assert(false,false)
  }

}