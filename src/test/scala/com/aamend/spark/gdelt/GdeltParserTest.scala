package com.aamend.spark.gdelt

import org.apache.spark.sql.Dataset
import org.scalatest.Matchers

import scala.io.Source

class GdeltParserTest extends SparkSpec with Matchers {

  sparkTest("loading GDELT universe") { spark =>

    import spark.implicits._

    // I simply test all my dataframes can be loaded, no exception should be thrown

    spark.read.gdeltEvent("/path/to/gkg")
    val gkg: Dataset[GKGEvent] = Source.fromInputStream(this.getClass.getResourceAsStream("gkg.csv")).getLines().toSeq.toDS().map(GdeltParser.parseGkg)
    gkg.show()

    val event: Dataset[Event] = Source.fromInputStream(this.getClass.getResourceAsStream("events.csv")).getLines().toSeq.toDS().map(GdeltParser.parseEvent)
    event.show()

    val mention: Dataset[Mention] = Source.fromInputStream(this.getClass.getResourceAsStream("mentions.csv")).getLines().toSeq.toDS().map(GdeltParser.parseMention)
    mention.show()

    val countryCodes: Dataset[CountryCode] = spark.loadCountryCodes
    countryCodes.show()

    val gcam: Dataset[GcamCode] = spark.loadGcams
    gcam.show()

    val cameoEvent: Dataset[CameoCode] = spark.loadCameoEventCodes
    cameoEvent.show()

    val cameoType: Dataset[CameoCode] = spark.loadCameoTypeCodes
    cameoType.show()

    val cameoGroup: Dataset[CameoCode] = spark.loadCameoGroupCodes
    cameoGroup.show()

    val cameoEthnic: Dataset[CameoCode] = spark.loadCameoEthnicCodes
    cameoEthnic.show()

    val cameoReligion: Dataset[CameoCode] = spark.loadCameoReligionCodes
    cameoReligion.show()

    val cameoCountry: Dataset[CameoCode] = spark.loadCameoCountryCodes
    cameoCountry.show()
  }

}
