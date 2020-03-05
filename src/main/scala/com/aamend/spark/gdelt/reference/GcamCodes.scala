package com.aamend.spark.gdelt.reference

import com.aamend.spark.gdelt.GcamCode
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object GcamCodes {

  def load(spark: SparkSession): Dataset[GcamCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("gcam.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      GcamCode(
        gcamCode = tokens(0),
        dictionaryId = tokens(1),
        dimensionId = tokens(2),
        dictionaryType = tokens(3),
        languageCode = tokens(4),
        dictionaryHumanName = tokens(5),
        dimensionHumanName = tokens(6),
        dictionaryCitation = tokens(7)
      )
    }).toDS()
  }

}

