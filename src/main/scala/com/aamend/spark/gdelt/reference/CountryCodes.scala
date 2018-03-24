package com.aamend.spark.gdelt.reference

import com.aamend.spark.gdelt.CountryCode
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object CountryCodes {

  def load(spark: SparkSession): Dataset[CountryCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("/countryInfo.txt")).getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CountryCode(
        iso = tokens(0),
        iso3 = tokens(1),
        isoNumeric = tokens(2),
        fips = tokens(3),
        country = tokens(4).toLowerCase()
      )
    }).toDS()
  }

}

