
package com.cts.spark.test


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

import org.apache.spark.sql.Row

import org.apache.spark.sql.DataFrame

object FixedLengthReader {
  
  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder().getOrCreate()
    
    // Path of actual data file
    val rdd = sparkSession.sparkContext.textFile("D:\\tmp\\fixed_length_data\\emp_data.txt")
    
    // Path of metadata file
    val metadata = sparkSession.read.option("header", "true").csv("D:\\tmp\\fixed_length_data\\metadata.csv")
    
    // Get the column names
    val header = metadata.select("col_name").rdd.map(x=>x.getString(0).trim()).collect()
    
    // Get all the data lengths
    val dataLength = metadata.select("size").rdd.map(x => x.getString(0).trim()).collect().map(y => y.toInt).toList
    
    // Create the StructField on field names
    val fields = header.map(fieldName => StructField(fieldName, StringType, nullable = true)) 
    val schema = StructType(fields)
    
    // Create DF by passing each line to the custom method which returns Row
    val outputDF = sparkSession.createDataFrame(rdd.map { x => extractFieldRecordFromLineRDD(dataLength,x) }, schema)
    
    outputDF.rdd.coalesce(1).saveAsTextFile("D:\\tmp\\output10")

  }
  
  def extractFieldRecordFromLineRDD (columns: List[Int], line: String): Row = {

    // Create an array with size equal to number of columns in metadata
    var fields = new Array[String](columns.length) 
    var startIndex = 0
    
    // Iterate over all the columns and extract the data accordingly
    for(x <- 0 to (columns.length-1)){
      // While reading via substring any field by it's length, if end index already becomes less then line length
      if((startIndex + columns(x)) > line.length){
        
        if (startIndex > line.length){ /*If startIndex only is greater than entire line length - this happens when last few fields are missing*/
          // Assign empty string to the last few fields
          fields(x) = ""
        }else { /*If the last present field is having less length than max defined as per schema*/
          // Assign substring till the end of this line
          fields(x) = line.substring(startIndex,line.length)
        }    
      } else {
        // Read fields (excluding the last filed present) by it's max length defined in schema
        fields(x) = line.substring(startIndex,(startIndex + columns(x)))
      }
      
      // Trim any unnecessary spaces in any of the fields
      if (fields(x) != null){
        fields(x) = fields(x).trim()
      }
      // Reset the startIndex
      startIndex = startIndex + columns(x)
    }
    Row.fromSeq(fields.toSeq)
  }
}