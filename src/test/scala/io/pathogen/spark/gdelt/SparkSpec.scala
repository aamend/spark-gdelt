package io.pathogen.spark.gdelt

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

trait SparkSpec extends FunSuite {

  def sparkTest(name: String)(f: SparkSession => Unit): Unit = {

    this.test(name) {

      val spark = SparkSession
        .builder()
        .appName(name)
        .master("local")
        .config("spark.default.parallelism", "1")
        .getOrCreate()

      try {
        f(spark)
      } finally {
        spark.stop()
      }
    }
  }
}