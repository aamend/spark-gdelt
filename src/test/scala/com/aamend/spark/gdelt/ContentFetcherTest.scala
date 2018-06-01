package com.aamend.spark.gdelt

import java.io.File

import com.gravity.goose.images.ImageUtils
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, Word2Vec}
import org.scalatest.Matchers

import scala.io.Source

class ContentFetcherTest extends SparkSpec with Matchers {

  sparkTest("testing E2E pipeline") { spark =>

    import spark.implicits._
    val df = List("https://www.theguardian.com/world/2018/jun/01/mariano-rajoy-ousted-as-spain-prime-minister").toDF("sourceUrl")

    val contentFetcher = new ContentFetcher()
      .setInputColumn("sourceUrl")
      .setOutputImageBase64Column("imageBase64")

    val pipeline = new Pipeline().setStages(Array(contentFetcher))
    val wordsDF = pipeline.fit(df).transform(df).drop("sourceUrl")

    wordsDF.show(false)
  }

}
