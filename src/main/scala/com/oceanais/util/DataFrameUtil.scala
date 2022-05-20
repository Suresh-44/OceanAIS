package com.oceanais.util

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

case class DataFrameUtil(val applicationConf: Config) {
  val spark = SparkSessionObject.spark
  val configVariables = new ConfigVariables(applicationConf)

  def Question3(): DataFrame = {
    val preProcessing_DF = spark.sql(configVariables.question3.getString("preProcessing"))
    preProcessing_DF.createOrReplaceTempView("preProcessing")
    val lead_df = spark.sql(configVariables.question3.getString("lead"))
    lead_df.createOrReplaceTempView("leaddf")
    val lag_df = spark.sql(configVariables.question3.getString("lag"))
    lag_df.createOrReplaceTempView("lagdf")
    val df_question3 = spark.sql(configVariables.question3.getString("main"))
    df_question3
  }

  def Question4():DataFrame = {
    // counting nulls in cargoDetails column
    val df_sparse_cargoDetails = spark.sql(configVariables.question4.getString("sparseCargoDetailsCount"))
    df_sparse_cargoDetails.createOrReplaceTempView("cargo_sparse")
    // counting nulls and zeros in imo column
    val df_sparse_imo = spark.sql(configVariables.question4.getString("sparseIMOCount"))
    df_sparse_imo.createOrReplaceTempView("imo_sparse")
    // counting nulls in destination column
    val df_sparse_destination = spark.sql(configVariables.question4.getString("sparseDestinationCount"))
    df_sparse_destination.createOrReplaceTempView("destination_sparse")
    val df_sparse = spark.sql(configVariables.question4.getString("main"))
    df_sparse
  }
  def Question5(): DataFrame = {
    // calculating region of the world and port names
    val df_question5 = spark.sql(configVariables.question5.getString("main"))
    df_question5
  }
  def Question6():DataFrame = {
    // Frequency Tabulation
    val df_question6 = spark.sql(configVariables.question6.getString("main"))
    df_question6
  }
  def Question9(dataFrame: DataFrame):DataFrame = {
    dataFrame.createOrReplaceTempView("q9")
    val df_question9 = spark.sql(configVariables.question9.getString("main"))
    df_question9
  }
  def Question10(dataFrameq3: DataFrame,dataFrameq9: DataFrame):DataFrame = {
    dataFrameq3.createOrReplaceTempView("q3")
    dataFrameq9.createOrReplaceTempView("q9")
    val preProcessing = spark.sql(configVariables.question10.getString("preProcessing"))
    preProcessing.createOrReplaceTempView("q9p")
    val df_q10 = spark.sql(configVariables.question10.getString("main"))
    df_q10
  }
  def Question11(dataFrameq3: DataFrame):DataFrame = {
    dataFrameq3.createOrReplaceTempView("q3")
    val df_question11 = spark.sql(configVariables.question11.getString("main"))
    df_question11
  }


/*
  def Question3(): DataFrame = {
    val preProcessing_DF = spark.sql("select mmsi,timestamp,navigation.navcode,navigation.navdesc,LEAD(navigation.navcode,1,-100) OVER (partition by mmsi ORDER BY timestamp) as navcode_lag_value,LAG(navigation.navcode,1,-100) OVER ( partition by mmsi ORDER BY timestamp) as navcode_lead_value from maintable")
    preProcessing_DF.createOrReplaceTempView("preProcessing")
    val lead_df = spark.sql("select mmsi,timestamp, navcode, navdesc,row_number() over(order by mmsi) as id from  preProcessing pp where pp.navcode !=pp.navcode_lead_value order by mmsi")
    lead_df.createOrReplaceTempView("leaddf")
    val lag_df = spark.sql("select mmsi,timestamp, navcode, navdesc,row_number() over(order by mmsi) as id from preProcessing pp where pp.navcode !=pp.navcode_lag_value order by mmsi ")
    lag_df.createOrReplaceTempView("lagdf")
    val df_question3 = spark.sql("select leaddf.mmsi,leaddf.timestamp as start_time,lagdf.timestamp as end_time,leaddf.navcode,leaddf.navdesc,round((unix_timestamp(lagdf.timestamp)-unix_timestamp(leaddf.timestamp))/3600,2) as TimePeriodInHours  from lagdf join leaddf on lagdf.id = leaddf.id order by mmsi")
    df_question3
  }

  def Question4():DataFrame = {
    // counting nulls in cargoDetails column
    val df_sparse_cargoDetails = spark.sql("select count(*) as cargodetails_sparse from maintable where cargodetails is null ")
    df_sparse_cargoDetails.createOrReplaceTempView("cargo_sparse")
    // counting nulls and zeros in imo column
    val df_sparse_imo = spark.sql("select count(*) as imo_sparse from maintable where imo is null or imo = 0 ")
    df_sparse_imo.createOrReplaceTempView("imo_sparse")
    // counting nulls in destination column
    val df_sparse_destination = spark.sql("select count(*) as destination_sparse from maintable where destination is null ")
    df_sparse_destination.createOrReplaceTempView("destination_sparse")
    val df_sparse = spark.sql("select * from cargo_sparse,imo_sparse,destination_sparse")
    df_sparse
  }

  def Question5(): DataFrame = {
    // calculating region of the world and port names
    val df_question5 = spark.sql("select mmsi,vesselDetails.flagCountry as country,port.name as port from maintable group by mmsi,country,port order by mmsi ")
    df_question5
  }

  def Question6():DataFrame = {
    // Frequency Tabulation
    val df_question6 = spark.sql("select navigation.navcode,navigation.navdesc,count(*) as frequency from maintable group by navigation.navcode,navigation.navdesc order by frequency desc")
    df_question6
  }

  def Question9(dataFrame: DataFrame):DataFrame = {
    dataFrame.createOrReplaceTempView("q9")
    val df_question9 = spark.sql("select * from q9 order by TimePeriodInHours desc limit 10")
    df_question9
  }

  def Question10(dataFrameq3: DataFrame,dataFrameq9: DataFrame):DataFrame = {
    dataFrameq3.createOrReplaceTempView("q3")
    dataFrameq9.createOrReplaceTempView("q9")
    val preProcessing = spark.sql("select avg(TimePeriodInHours) as ld from q9")
    preProcessing.createOrReplaceTempView("q9p")
    val df_q10 = spark.sql("select q3.mmsi,q3.start_time,q3.end_time,q3.navcode,q3.navdesc,if(q3.TimePeriodInHours>q9p.ld,'increased','decreased') as state from q3,q9p ")
    df_q10
  }

  def Question11(dataFrameq3: DataFrame):DataFrame = {
    dataFrameq3.createOrReplaceTempView("q3")
    val df_question11 = spark.sql("select mmsi,round(dwell_Time_In_Hours/frequency,2) as average_dwell_InHours from (select mmsi,sum(TimePeriodInHours) as dwell_Time_In_Hours,count(*) as frequency from q3 where navdesc='At Anchor' or navdesc='Moored' group by mmsi order by mmsi)")
    df_question11
  }

 */

}
