package com.aamend.spark.gdelt

import org.apache.spark.ml.Pipeline
import org.scalatest._
import flatspec._
import matchers._

class ContentFetcherTest extends SparkSpec with should.Matchers {

  sparkTest("testing E2E pipeline") { spark =>

    import spark.implicits._
    val gdeltDf = List("https://www.theguardian.com/world/2018/jun/01/mariano-rajoy-ousted-as-spain-prime-minister").toDF("sourceUrl")

    val contentFetcher = new ContentFetcher()
      .setInputCol("sourceUrl")
      .setOutputImageUrlCol("imageUrl")
      .setOutputImageBase64Col("imageBase64")
      .setImagemagickConvert("/usr/local/bin/convert")
      .setImagemagickIdentify("/usr/local/bin/identify")

    val contentDF = contentFetcher.transform(gdeltDf)
    contentDF.show()
  }

}
