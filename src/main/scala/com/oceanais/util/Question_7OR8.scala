package com.oceanais.util

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

case class Question_7OR8(val applicationConf: Config) {
  val spark = SparkSessionObject.spark
  val configVariables = new ConfigVariables(applicationConf)

  def Question_a(mmsi:Long): DataFrame ={
    //var query = configVariables.question_a.getString("main")
    //query = query.replace("@@mmsi",mmsi.toString)
    val df_question7a = spark.sql(configVariables.question_a.getString("main").replace("@@mmsi",mmsi.toString))
    df_question7a
  }

  def Question_b(mmsi:Long):DataFrame={
    val df_preprocessing = spark.sql(configVariables.question_b.getString("preProcessing").replace("@@mmsi",mmsi.toString))
    df_preprocessing.createOrReplaceTempView("q7b")
    val df_lead = spark.sql(configVariables.question_b.getString("lead"))
    df_lead.createOrReplaceTempView("leaddf")
    val df_lag = spark.sql(configVariables.question_b.getString("lag"))
    df_lag.createOrReplaceTempView("lagdf")
    val df_question7b = spark.sql(configVariables.question_b.getString("main"))
    df_question7b
  }

  def Question_c(dataFrame: DataFrame):DataFrame = {
    dataFrame.createOrReplaceTempView("q7c")
    val df_question7c = spark.sql(configVariables.question_c.getString("main"))
    df_question7c
  }

}
