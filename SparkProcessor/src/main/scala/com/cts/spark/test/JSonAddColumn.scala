
package com.cts.spark.test


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JSonAddColumn {
  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder().getOrCreate()

    val input = sparkSession.read.option("multiLine", true).option("mode", "PERMISSIVE").json("D:\\tmp\\sample.json")
    
    val ingestedDate = java.time.LocalDate.now
    val output = input.withColumn("timestamp",lit(ingestedDate.toString()))
     
    output.write.json("D:\\tmp\\out-sandip")

  }
}