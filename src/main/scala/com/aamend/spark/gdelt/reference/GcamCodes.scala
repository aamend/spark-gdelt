package com.aamend.spark.gdelt.reference

import com.aamend.spark.gdelt.GcamCode
import com.aamend.spark.gdelt.T
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object GcamCodes {

  def load(spark: SparkSession): Dataset[GcamCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("gcam.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      GcamCode(
        gcamCode = T(()=>tokens(0)),
        dictionaryId = T(()=>tokens(1)),
        dimensionId = T(()=>tokens(2)),
        dictionaryType = T(()=>tokens(3)),
        languageCode = T(()=>tokens(4)),
        dictionaryHumanName = T(()=>tokens(5)),
        dimensionHumanName = T(()=>tokens(6)),
        dictionaryCitation = T(()=>tokens(7))
      )
    }).toDS()
  }

}

