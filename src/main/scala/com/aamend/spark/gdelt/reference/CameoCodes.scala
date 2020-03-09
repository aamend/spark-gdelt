package com.aamend.spark.gdelt.reference

import com.aamend.spark.gdelt.CameoCode
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object CameoCodes {

  def loadEventCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("cameoEvent.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadTypeCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("cameoType.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadGroupCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("cameoGroup.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadEthnicCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("cameoEthnic.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadReligionCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("cameoReligion.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }

  def loadCountryCode(spark: SparkSession): Dataset[CameoCode] = {
    import spark.implicits._
    Source.fromInputStream(this.getClass.getResourceAsStream("cameoCountry.txt"), "UTF-8").getLines().toSeq.drop(1).map(line => {
      val tokens = line.split("\t")
      CameoCode(
        cameoCode = tokens(0).toUpperCase(),
        cameoValue = tokens(1).toLowerCase()
      )
    }).toDS()
  }
}

