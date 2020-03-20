package com.aamend.spark.gdelt.reference

import com.aamend.spark.gdelt.CountryCode
import com.aamend.spark.gdelt.T
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object CountryCodes {

  def load(spark: SparkSession): Dataset[CountryCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("countryInfo.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CountryCode(
        iso = T(()=>tokens(0)),
        iso3 = T(()=>tokens(1)),
        isoNumeric = T(()=>tokens(2)),
        fips = T(()=>tokens(3)),
        country = T(()=>tokens(4).toLowerCase())
      )
    }).toDS()
  }

}

