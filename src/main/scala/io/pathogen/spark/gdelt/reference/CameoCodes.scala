package io.pathogen.spark.gdelt.reference

import io.pathogen.spark.gdelt.CameoCode
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object CameoCodes {

  def loadEventCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("/cameoEvent.txt")).getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadTypeCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("/cameoType.txt")).getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadGroupCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("/cameoGroup.txt")).getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadEthnicCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("/cameoEthnic.txt")).getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadReligionCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("/cameoReligion.txt")).getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadCountryCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("/cameoCountry.txt")).getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }
}

