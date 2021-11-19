package com.aamend.spark.gdelt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

trait SparkSpec extends AnyFunSuite {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def sparkTest(name: String)(f: SparkSession => Unit): Unit = {

    this.test(name) {

      val spark = SparkSession
        .builder()
        .appName(name)
        .master("local")
        .config("spark.default.parallelism", "1")
        .config("spark.testing.memory", "500000000")
        .getOrCreate()

      try {
        f(spark)
      } finally {
        spark.stop()
      }
    }
  }
}
