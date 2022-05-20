package com.oceanais.util

import com.typesafe.config.{Config, ConfigFactory}

class ConfigVariables(val applicationConf: Config) {
  val question3 = applicationConf.getConfig("Question3")
  val question4 = applicationConf.getConfig("Question4")
  val question5 = applicationConf.getConfig("Question5")
  val question_7OR8 = applicationConf.getConfig("Question_7OR8")
  val question_a  = question_7OR8.getConfig("Question_a")
  val question_b  = question_7OR8.getConfig("Question_b")
  val question_c  = question_7OR8.getConfig("Question_c")
  val question6 = applicationConf.getConfig("Question6")
  val question9 = applicationConf.getConfig("Question9")
  val question10 = applicationConf.getConfig("Question10")
  val question11 = applicationConf.getConfig("Question11")
}
