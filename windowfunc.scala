package com.abc.spark1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object windowfunc {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\winutils")

    {
      val spark = SparkSession.builder().master("local[2]").appName("test").getOrCreate()

      import spark.implicits._

      val df = spark.read.option("header","true").option("inferschema","true").csv("C:\\Users\\SRIKANTH\\Downloads\\MOCK_DATA.csv") .toDF()
      df.printSchema()
      val df1 = df.orderBy("dept")
      df1.show(30)
      //df.show()

      val dff = Window.partitionBy("dept")

      val dff1 = df.withColumn("sal", max("sal").over(dff) - df("sal"))
      val dff2 = df.withColumn("lead", lead('sal,1).over(dff)).withColumn("lag",lag('sal,1).over(dff))
      dff2.show(50)
      dff1.show()






    }

  }
}