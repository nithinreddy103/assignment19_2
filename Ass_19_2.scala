package com.spark.streaming

import org.apache.spark.sql.SparkSession

object Assignment_19_2 extends App {

  val sparkSession = SparkSession.builder.master("local").appName("spark").config("spark.sql.warehouse.dir","file:///C:/Users").getOrCreate()
  val csvDF1 =   sparkSession.sqlContext.read.format("csv")
    .option("header", "true")
    .option("inferSchema", true).load("C:\\Users\\user\\Downloads\\Sports_data.txt")

  csvDF1.createOrReplaceTempView("Sports")
  //1)
  def alterName = org.apache.spark.sql.functions.udf((first_name:String,last_name:String)=>{"Mr." + first_name.substring(0,2) + " " +last_name})
  csvDF1.withColumn("UpdatedName",alterName(csvDF1("firstname"),csvDF1("lastname"))).show()

  //2)
  def category = org.apache.spark.sql.functions.udf((medaltype:String,age:String)=>
 {if((medaltype == "gold") && (age.toInt>=32)) "pro"
   else if(medaltype == "gold" && age.toInt<=31) "amateur"
   else if(medaltype == "silver" && age.toInt>=32) "expert"
   else if(medaltype == "silver" && age.toInt<=31) "rookie"})

  csvDF1.withColumn("CategoryType",category(csvDF1("medal_type"),csvDF1("age"))).show()
}
