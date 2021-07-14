
package com.cts.spark.test


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CountDupAndDedupRecord {
  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder().getOrCreate()
    
    import sparkSession.sqlContext.implicits._
    
    val accDF = List(("1", "Acc1"), ("1", "Acc1"), ("1", "Acc1"), ("2", "Acc2"), ("2", "Acc2"), ("3", "Acc3")).toDF("AccId", "AccName")  
    val outputDF = accDF.groupBy($"accId",$"accName").count
    
    outputDF.rdd.coalesce(1).saveAsTextFile("D:\\tmp\\output1")

  }
}