
package com.cts.spark.test


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FlattenRecords {
  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.sqlContext.implicits._
    
    val rdd1 = sparkSession.read.option("header", "true").csv("D:\\tmp\\normalized_src_feeds\\employee_basic_details.csv")
    
    val empBasicDF = rdd1.select("emp_id","emp_name","age","email","marital_status").as("empBasic")
    
    val rdd2 = sparkSession.read.option("header", "true").csv("D:\\tmp\\normalized_src_feeds\\employee_address_details.csv")
    
    val empAddressDF = rdd2.select("emp_id","city","country").as("empAddress")
    
    val rdd3 = sparkSession.read.option("header", "true").csv("D:\\tmp\\normalized_src_feeds\\employee_employment_details.csv")
    
    val empEmploymentDF = rdd3.select("emp_id","company_worked_for","tenure").as("empEmployment")
    
    val empCombinedDF = empBasicDF.join(empAddressDF, $"empBasic.emp_id"===$"empAddress.emp_id").join(empEmploymentDF,$"empBasic.emp_id"===$"empEmployment.emp_id","right").select("empBasic.emp_id","empBasic.emp_name","empBasic.age","empBasic.email","empBasic.marital_status","empAddress.city","empAddress.country","empEmployment.company_worked_for","empEmployment.tenure")
    
    empCombinedDF.write.option("header","true").csv("D:\\tmp\\output11")              

  }
}