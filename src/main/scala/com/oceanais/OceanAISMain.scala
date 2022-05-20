package com.oceanais

import com.oceanais.util.{DataFrameUtil, Question_7OR8, SparkSessionObject}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}

object OceanAISMain {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSessionObject.spark

    val applicationConf: Config = ConfigFactory.load("\\config\\queries.json")

    val df = spark.read.parquet(args(0))
    df.createOrReplaceTempView("test")
    df.printSchema()
    df.show(false)

    val df_timestamp = spark.sql("select from_unixtime(epochMillis/1000,'yyyy-MM-dd HH:mm:ss') as timestamp, * from test")
    df_timestamp.show(false)
    df_timestamp.createOrReplaceTempView("MainTable")

    val dataFrameUtil = DataFrameUtil(applicationConf)

    // Question 3
    println("******** Q3 **********")
    val df_question3 = dataFrameUtil.Question3()
    df_question3.show(false)


    // Question 4
    println("******** Q4 **********")
    val df_question4 = dataFrameUtil.Question4()
    df_question4.show(false)


    // Question 5
    println("******** Q5 **********")
    val df_question5 = dataFrameUtil.Question5()
    df_question5.show(false)

    //Question 6
    println("******** Q6 **********")
    val df_question6 = dataFrameUtil.Question6()
    df_question6.show(false)

    //Question 7
    val df_question7 = Question_7OR8(applicationConf)
    println("******** Q7a **********")
    val df_question7a = df_question7.Question_a(205792000)
    df_question7a.show(false)
    println("******** Q7b **********")
    val df_question7b = df_question7.Question_b(205792000)
    df_question7b.show(false)
    println("******** Q7c **********")
    val df_question7c = df_question7.Question_c(df_question7b)
    df_question7c.show(false)

    //Question 8
    val df_question8 = Question_7OR8(applicationConf)
    println("******** Q8a **********")
    val df_question8a = df_question8.Question_a(413970021)
    df_question8a.show(false)
    println("******** Q8b **********")
    val df_question8b = df_question8.Question_b(413970021)
    df_question8b.show(false)
    println("******** Q8c **********")
    val df_question8c = df_question8.Question_c(df_question8b)
    df_question8c.show(false)

    // Question 9
    println("******** Q9 **********")
    val df_question9 = dataFrameUtil.Question9(df_question3)
    df_question9.show(false)

    // Question 10
    println("******** Q10 **********")
    val df_question10 = dataFrameUtil.Question10(df_question3, df_question9)
    df_question10.show(false)

    // Question 11
    println("******** Q11 **********")
    val df_question11 = dataFrameUtil.Question11(df_question3)
    df_question11.show(false)

  }
}
