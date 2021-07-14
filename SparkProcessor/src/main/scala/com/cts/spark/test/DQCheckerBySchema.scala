
package com.cts.spark.test


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types._

object DQCheckerBySchema {
  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.sqlContext.implicits._
     
     // Path of metadata file
    val metadata = sparkSession.read.option("header", "true").csv("D:\\tmp\\schema-checker\\metadata.csv")
    
    // Get the column names
    val columnName = metadata.select("col_name").rdd.map(x => x.getString(0).trim()).collect().map(y => y.toString).toList
    
    // Get all the data types
    val dataType = metadata.select("data_type").rdd.map(x => x.getString(0).trim()).collect().map(y => y.toString).toList
    
    // Get all the nullability indicators
    val nullable = metadata.select("nullable").rdd.map(x => x.getString(0).trim()).collect().map(y => y.toString).toList
    
    // Create an array with size equal to number of columns in metadata
    var structFields = new Array[StructField](columnName.length) 
    
    // Map between user data type and spark data type
    val dataTypeMap = Map("integer" -> IntegerType, "string" -> StringType)
    
    // Map between user nullable specification and spark nullable specification
    val nullableMap = Map("N" -> false, "Y" -> true)
    
    for(x <- 0 to (columnName.length-1)){
    	structFields(x)=new StructField(columnName.apply(x), dataTypeMap(dataType.apply(x)), nullableMap(nullable.apply(x)))
    }
    
    val customSchema = new StructType(structFields)
    val rdd = spark.read.option("header", "true").option("mode", "DROPMALFORMED").schema(customSchema).csv("D:\\tmp\\schema-checker\\sample.csv")
    
    rdd.coalesce(1).saveAsTextFile("D:\\tmp\\output101")

  }
  
  
}