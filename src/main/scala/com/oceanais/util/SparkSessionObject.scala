package com.oceanais.util

import org.apache.spark.sql.SparkSession

object SparkSessionObject {
  val spark = SparkSession.builder().appName(("OceanAIS")).master("local[*]")
    .getOrCreate()
}
