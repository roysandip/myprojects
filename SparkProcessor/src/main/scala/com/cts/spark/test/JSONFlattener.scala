
package com.cts.spark.test


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.DataFrame

object JSONFlattener {
  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder().getOrCreate()

    val input = sparkSession.read.option("multiLine", true).option("mode", "PERMISSIVE").json("D:\\tmp\\mysample.json")
    val schema = input.schema
    val output = schema.json
    
    val flattendedJSON = flattenDataframe(input)
  //schema of the JSON after Flattening
  flattendedJSON.schema
  
  //Output DataFrame After Flattening
  flattendedJSON.show(false)



  }
  
  def flattenDataframe(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    
    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
         // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
         val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }
}