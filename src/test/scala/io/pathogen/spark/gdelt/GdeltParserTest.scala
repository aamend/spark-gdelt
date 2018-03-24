package io.pathogen.spark.gdelt

import org.scalatest.Matchers

import scala.io.Source

class GdeltParserTest extends SparkSpec with Matchers {

  sparkTest("gdelt parser") { spark =>

    import spark.implicits._

    val gkg = Source.fromInputStream(this.getClass.getResourceAsStream("gkg.csv")).getLines().toSeq.toDS().map(GdeltParser.parseGkg)
    gkg.show()

    val event = Source.fromInputStream(this.getClass.getResourceAsStream("events.csv")).getLines().toSeq.toDS().map(GdeltParser.parseEvent)
    event.show()

    val mention = Source.fromInputStream(this.getClass.getResourceAsStream("mentions.csv")).getLines().toSeq.toDS().map(GdeltParser.parseMention)
    mention.show()

    val countryCodes = spark.loadCountryCodes
    countryCodes.show()

    val gcam = spark.loadGcams
    gcam.show()

    val cameoEvent = spark.loadCameoEventCodes
    cameoEvent.show()

    val cameoType = spark.loadCameoTypeCodes
    cameoType.show()

    val cameoGroup = spark.loadCameoGroupCodes
    cameoGroup.show()

    val cameoEthnic = spark.loadCameoEthnicCodes
    cameoEthnic.show()

    val cameoReligion = spark.loadCameoReligionCodes
    cameoReligion.show()

    val cameoCountry = spark.loadCameoCountryCodes
    cameoCountry.show()
  }

}
