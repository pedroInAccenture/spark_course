package com.course.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object Operation {

    def addColumnRowNumber(df:DataFrame)= {
      val windowSpec = Window.partitionBy("department").orderBy("salary")
      df.withColumn("row_number", row_number.over(windowSpec))
    }

}
