package com.course.common

import com.typesafe.config.{Config, ConfigFactory}

object LoadConf {

  def getConfig:Config = ConfigFactory.load("config/application.conf").getConfig(System.getProperty("env"))

  def main(args: Array[String]): Unit = {
    println(s">>>>>> config: ${System.getProperty("env")}")
    print(getConfig.toString)
  }
}
