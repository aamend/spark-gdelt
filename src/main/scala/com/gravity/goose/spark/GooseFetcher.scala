package com.gravity.goose.spark

import com.gravity.goose.{Configuration, Goose}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait GooseFetcherParams extends Params with DefaultParamsWritable {
  val annotators = new Param[Map[String, String]](this, "annotators", s"The list of annotators [${ANNOTATORS.mkString(",")}]")
  val urlColumn = new Param[String](this, "urlColumn", "The input column containing URLs")
  val userAgent = new Param[String](this, "userAgent", "User agent that is sent with your web requests to extract URL content")
  val socketTimeout = new Param[Int](this, "socketTimeout", "Socket timeout (ms)")
  val connectionTimeout = new Param[Int](this, "connectionTimeout", "Connection timeout (ms)")
  val enableImageFetching = new Param[Boolean](this, "enableImageFetching", "(Experimental) Fetching image header as base64")
}

class GooseFetcher(override val uid: String) extends Transformer with GooseFetcherParams {

  def setAnnotators(value: Map[String, String]): this.type = {
    require(value.nonEmpty, "At least one annotator must be provided")
    require(value.values.toSet.size == value.keys.size, "Annotator fields must be unique")
    value.keys.foreach(annotator => require(ANNOTATORS.contains(annotator), s"Annotator [$annotator] is not valid, supported are [${ANNOTATORS.mkString(",")}]"))
    set(annotators, value)
  }

  setDefault(annotators -> ANNOTATORS.zip(ANNOTATORS).toMap)

  def setUserAgent(value: String): this.type = set(userAgent, value)

  setDefault(userAgent -> "Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8")

  def setSocketTimeout(value: Int): this.type = set(socketTimeout, value)

  setDefault(socketTimeout -> 10000)

  def setConnectionTimeout(value: Int): this.type = set(connectionTimeout, value)

  setDefault(connectionTimeout -> 10000)

  def setEnableImageFetching(value: Boolean): this.type = set(enableImageFetching, value)

  setDefault(enableImageFetching -> false)

  def setUrlColumn(value: String): this.type = set(urlColumn, value)

  setDefault(urlColumn -> "url")

  def this() = this(Identifiable.randomUID("goose"))

  override def transform(origDS: Dataset[_]): DataFrame = {

    // Make sure the URL field exist
    require(origDS.schema.exists(s => s.name == $(urlColumn) && s.dataType == StringType), "Field [" + $(urlColumn) + "] is not valid")

    // Make sure annotators field do not exist
    $(annotators).values.foreach(annotator => {
      require(!origDS.schema.exists(s => s.name == annotator), s"Annotator field [$annotator] already exist")
    })

    // This intermediate dataset to make sure we don't scrape more than once a same URL
    val urlDF = origDS.select($(urlColumn)).dropDuplicates($(urlColumn))

    // Append URL dataframe with article annotators
    val urlContentRDD = urlDF.rdd.mapPartitions(rows => {

      // Initialize Goose only once for each partition
      val conf = new Configuration()
      conf.setEnableImageFetching($(enableImageFetching))
      conf.setBrowserUserAgent($(userAgent))
      conf.setSocketTimeout($(socketTimeout))
      conf.setConnectionTimeout($(connectionTimeout))
      val goose = new Goose(conf)

      // Scrape each URL individually
      val articles = scrapeArticles(rows.map(_.getAs[String]($(urlColumn))), goose)

      // Convert articles as Row
      articles.map(article => {
        val appended: Seq[Any] = $(annotators).map { case (key, _) =>
          key match {
            case ANNOTATOR_TITLE => article.title.getOrElse("")
            case ANNOTATOR_DESCRIPTION => article.description.getOrElse("")
            case ANNOTATOR_CONTENT => article.content.getOrElse("")
            case ANNOTATOR_KEYWORDS => article.keywords
            case ANNOTATOR_PUBLISH_DATE => article.publishDate.orNull
          }
        }.toSeq
        Row.fromSeq(Seq(article.url) ++ appended)
      })
    })

    // Transform RDD of Row to Dataframe
    val contentDF = origDS.sqlContext.createDataFrame(urlContentRDD, transformSchema(urlDF.schema))

    // Join articles back to any duplicate URL dataset
    contentDF.join(origDS, List($(urlColumn)))

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.seq ++ $(annotators).map { case (key, value) =>
        key match {
          case ANNOTATOR_TITLE => StructField(value, StringType, nullable = false)
          case ANNOTATOR_DESCRIPTION => StructField(value, StringType, nullable = false)
          case ANNOTATOR_CONTENT => StructField(value, StringType, nullable = false)
          case ANNOTATOR_KEYWORDS => StructField(value, ArrayType.apply(StringType), nullable = false)
          case ANNOTATOR_PUBLISH_DATE => StructField(value, DateType, nullable = true)
        }
      }
    )
  }

  override def copy(extra: ParamMap): Transformer = {
    defaultCopy(extra)
  }
}

object GooseFetcher extends DefaultParamsReadable[GooseFetcher] {
  override def load(path: String): GooseFetcher = super.load(path)
}
