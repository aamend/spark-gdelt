package com.aamend.spark.gdelt

import org.scalatest.Matchers

import scala.io.Source

class GdeltParserTest extends SparkSpec with Matchers {

  // I simply test all my dataframes can be loaded, no exception should be thrown
  sparkTest("loading GDELT universe") { spark =>
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("gkg.csv")).getLines().toSeq.toDS().map(GdeltParser.parseGkg).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("events.csv")).getLines().toSeq.toDS().map(GdeltParser.parseEvent).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("mentions.csv")).getLines().toSeq.toDS().map(GdeltParser.parseMention).show()
  }

  sparkTest("loading GDELT events") { spark =>
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("events.csv")).getLines().toSeq.toDS().map(GdeltParser.parseEvent).select("actor2Geo.geoName").show()
  }


  // I simply test all my dataframes can be loaded, no exception should be thrown
  sparkTest("loading GDELT reference data") { spark =>
    spark.loadCountryCodes.show()
    spark.loadGcams.show()
    spark.loadCameoEventCodes.show()
    spark.loadCameoTypeCodes.show()
    spark.loadCameoGroupCodes.show()
    spark.loadCameoEthnicCodes.show()
    spark.loadCameoReligionCodes.show()
    spark.loadCameoCountryCodes.show()
  }

}
