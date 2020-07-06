package com.aamend.spark.gdelt

import org.scalatest.Matchers

import scala.io.Source

class GdeltParserTest extends SparkSpec with Matchers {

  //   I simply test all my dataframes can be loaded, no exception should be thrown
  sparkTest("loading GDELT universe") { spark =>
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("gkg.csv"), "UTF-8").getLines().toSeq.toDS().map(GdeltParser.parseGkg).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("gkgT.csv"), "UTF-8").getLines().toSeq.toDS().map(GdeltParser.parseGkg).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("gkg1.csv"), "UTF-8").getLines().toSeq.toDS().map(GdeltParser.parseGkgV1).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("events.csv"), "UTF-8").getLines().toSeq.toDS().map(GdeltParser.parseEvent).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("events1.csv"), "UTF-8").getLines().toSeq.toDS().map(GdeltParser.parseEventV1).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("eventsT.csv"), "UTF-8").getLines().toSeq.toDS().map(GdeltParser.parseEvent).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("mentions.csv"), "UTF-8").getLines().toSeq.toDS().map(GdeltParser.parseMention).show()
    Source.fromInputStream(this.getClass.getResourceAsStream("mentionsT.csv"), "UTF-8").getLines().toSeq.toDS().map(GdeltParser.parseMention).show()
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
