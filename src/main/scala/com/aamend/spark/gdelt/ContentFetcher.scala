package com.aamend.spark.gdelt

import java.io.File

import com.gravity.goose.{Configuration, Goose}
import org.apache.commons.lang.StringUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util.Try

trait ContentFetcherParams extends Params with DefaultParamsWritable {
  val inputColumn = new Param[String](this, "inputColumn", "(MANDATORY) The input column containing URLs")
  val outputContentColumn = new Param[String](this, "outputContentColumn", "(OPTIONAL) Field that will contain HTML content")
  val outputTitleColumn = new Param[String](this, "outputTitleColumn", "(OPTIONAL) Field that will contain HTML title")
  val outputDescriptionColumn = new Param[String](this, "outputDescriptionColumn", "(OPTIONAL) Field that will contain HTML description")
  val outputKeywordsColumn = new Param[String](this, "outputKeywordsColumn", "(OPTIONAL) Field that will contain HTML keywords")
  val outputPublishDateColumn = new Param[String](this, "outputPublishDateColumn", "(OPTIONAL) Field that will contain HTML publishDate")
  val outputImageUrlColumn = new Param[String](this, "outputImageUrlColumn", "(OPTIONAL) Field that will contain HTML image header URL")
  val outputImageBase64Column = new Param[String](this, "outputImageBase64Column", "(OPTIONAL) Field that will contain HTML image header in Base64")
  val userAgent = new Param[String](this, "userAgent", "(OPTIONAL) User agent that is sent with your web requests to extract URL content")
  val socketTimeout = new Param[Int](this, "socketTimeout", "(OPTIONAL) Socket timeout (ms)")
  val connectionTimeout = new Param[Int](this, "connectionTimeout", "(OPTIONAL) Connection timeout (ms)")
  val imagemagickConvert = new Param[String](this, "imagemagickConvert", "(OPTIONAL) imagemagick convert executable")
  val imagemagickIdentify = new Param[String](this, "imagemagickIdentify", "(OPTIONAL) imagemagick identify executable")
}

class ContentFetcher(override val uid: String) extends Transformer with ContentFetcherParams {

  def setImagemagickConvert(value: String): this.type = set(imagemagickConvert, value)

  setDefault(imagemagickConvert -> "/usr/local/bin/convert")

  def setImagemagickIdentify(value: String): this.type = set(imagemagickIdentify, value)

  setDefault(imagemagickIdentify -> "/usr/local/bin/identify")

  def setUserAgent(value: String): this.type = set(userAgent, value)

  setDefault(userAgent -> "Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8")

  def setSocketTimeout(value: Int): this.type = set(socketTimeout, value)

  setDefault(socketTimeout -> 10000)

  def setConnectionTimeout(value: Int): this.type = set(connectionTimeout, value)

  setDefault(connectionTimeout -> 10000)

  def setInputCol(value: String): this.type = set(inputColumn, value)

  setDefault(inputColumn -> "sourceURL")

  def setOutputContentCol(value: String): this.type = set(outputContentColumn, value)

  setDefault(outputContentColumn -> "")

  def setOutputTitleCol(value: String): this.type = set(outputTitleColumn, value)

  setDefault(outputTitleColumn -> "")

  def setOutputDescriptionCol(value: String): this.type = set(outputDescriptionColumn, value)

  setDefault(outputDescriptionColumn -> "")

  def setOutputKeywordsCol(value: String): this.type = set(outputKeywordsColumn, value)

  setDefault(outputKeywordsColumn -> "")

  def setOutputPublishDateCol(value: String): this.type = set(outputPublishDateColumn, value)

  setDefault(outputPublishDateColumn -> "")

  def setOutputImageUrlCol(value: String): this.type = set(outputImageUrlColumn, value)

  setDefault(outputImageUrlColumn -> "")

  def setOutputImageBase64Col(value: String): this.type = set(outputImageBase64Column, value)

  setDefault(outputImageBase64Column -> "")

  def this() = this(Identifiable.randomUID("com/gravity/goose"))

  override def transform(origDS: Dataset[_]): DataFrame = {

    val outputFields = loadOutputFields()

    // Make sure the URL field exist
    require(origDS.schema.exists(s => s.name == $(inputColumn) && s.dataType == StringType), "Field [" + $(inputColumn) + "] is not valid")

    // Make sure at least one output field is specified
    require(outputFields.nonEmpty, "At least one output field should be specified")

    // Make sure each specified output field does not exist
    outputFields.values.foreach(outputField => require(!origDS.schema.exists(_.name == outputField), s"Field [$outputField] already exist"))

    // This intermediate dataset to make sure we don't scrape more than once a same URL
    val urlDF = origDS.select($(inputColumn)).dropDuplicates($(inputColumn))

    // If Image fetching enabled, we need path to image magic and convert
    if(StringUtils.isNotEmpty($(outputImageUrlColumn)) || StringUtils.isNotEmpty($(outputImageBase64Column))) {
      require(StringUtils.isNotEmpty($(imagemagickConvert)) && Try(new File($(imagemagickConvert))).isSuccess, "imagemagick convert executable needs to be specified for Image fetching")
      require(StringUtils.isNotEmpty($(imagemagickIdentify)) && Try(new File($(imagemagickIdentify))).isSuccess, "imagemagick identify executable needs to be specified for Image fetching")
    }

    // Append URL dataframe with article annotators
    val urlContentRDD = urlDF.rdd.mapPartitions(rows => {

      // Initialize Goose only once for each partition
      val conf = new Configuration()
      if(StringUtils.isNotEmpty($(outputImageUrlColumn)) || StringUtils.isNotEmpty($(outputImageBase64Column))) {
        conf.setEnableImageFetching(true)
        conf.setImagemagickConvertPath($(imagemagickConvert))
        conf.setImagemagickIdentifyPath($(imagemagickIdentify))
      } else {
        conf.setEnableImageFetching(false)
      }
      conf.setBrowserUserAgent($(userAgent))
      conf.setSocketTimeout($(socketTimeout))
      conf.setConnectionTimeout($(connectionTimeout))
      val goose = new Goose(conf)

      // Scrape each URL individually
      val articles = scrapeContent(rows.map(_.getAs[String]($(inputColumn))), goose)

      // Convert articles as Row
      articles.map(article => {
        val appended: Seq[Any] = outputFields.map { case (key, _) =>
          key match {
            case ANNOTATOR_TITLE => article.title.getOrElse("")
            case ANNOTATOR_DESCRIPTION => article.description.getOrElse("")
            case ANNOTATOR_CONTENT => article.content.getOrElse("")
            case ANNOTATOR_KEYWORDS => article.keywords
            case ANNOTATOR_PUBLISH_DATE => article.publishDate.orNull
            case ANNOTATOR_IMAGE_URL => article.imageURL.getOrElse("")
            case ANNOTATOR_IMAGE_BASE64 => article.imageBase64.getOrElse("")
          }
        }.toSeq
        Row.fromSeq(Seq(article.url) ++ appended)
      })
    })

    // Transform RDD of Row to Dataframe
    val contentDF = origDS.sqlContext.createDataFrame(urlContentRDD, transformSchema(urlDF.schema))

    // Join articles back to any duplicate URL dataset
    contentDF.join(origDS, List($(inputColumn)))

  }

  private def loadOutputFields(): Map[String, String] = {
    Map(
      ANNOTATOR_TITLE -> $(outputTitleColumn),
      ANNOTATOR_DESCRIPTION -> $(outputDescriptionColumn),
      ANNOTATOR_CONTENT -> $(outputContentColumn),
      ANNOTATOR_KEYWORDS -> $(outputKeywordsColumn),
      ANNOTATOR_PUBLISH_DATE -> $(outputPublishDateColumn),
      ANNOTATOR_IMAGE_BASE64 -> $(outputImageBase64Column),
      ANNOTATOR_IMAGE_URL -> $(outputImageUrlColumn)
    ).filter(s => StringUtils.isNotEmpty(s._2))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.seq ++ loadOutputFields().map { case (key, value) =>
        key match {
          case ANNOTATOR_TITLE => StructField(value, StringType, nullable = false)
          case ANNOTATOR_DESCRIPTION => StructField(value, StringType, nullable = false)
          case ANNOTATOR_CONTENT => StructField(value, StringType, nullable = false)
          case ANNOTATOR_KEYWORDS => StructField(value, ArrayType.apply(StringType), nullable = false)
          case ANNOTATOR_PUBLISH_DATE => StructField(value, DateType, nullable = true)
          case ANNOTATOR_IMAGE_URL => StructField(value, StringType, nullable = true)
          case ANNOTATOR_IMAGE_BASE64 => StructField(value, StringType, nullable = true)
        }
      }
    )
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }
}

object ContentFetcher extends DefaultParamsReadable[ContentFetcher] {
  override def load(path: String): ContentFetcher = super.load(path)
}
