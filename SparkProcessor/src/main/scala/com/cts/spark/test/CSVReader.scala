package com.cts.spark.test


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object CSVReader {
	def main(args: Array[String]) {

		val sparkSession = SparkSession.builder().getOrCreate()

				// Path of actual data file
				val inputRDD = sparkSession.sparkContext.textFile("D:\\tmp\\quoted_csv_data\\emp_data.csv")

				val header = inputRDD.first()

//				val csvData = inputRDD.take(inputRDD.count.toInt).drop(1).dropRight(1)
				val csvDataWithFooter = inputRDD.filter(record=>record != header)
				
				val indexedCSVData = csvDataWithFooter.zipWithIndex
				
				val count = indexedCSVData.count
				
				val cleansedCSVData = indexedCSVData.filter(row => row._2 < count - 1).map(row1 => row1._1)

				val outputRDD = cleansedCSVData.map(row=>row.split(",").map(cell=> {if(cell.length > 2 && cell.charAt(0) == '"' && cell.charAt(cell.length - 1) == '"'){ cell.substring(1, cell.length - 1);}}).mkString(","))
				
				
				outputRDD.coalesce(1).saveAsTextFile("D:\\tmp\\output10")
	}   
}