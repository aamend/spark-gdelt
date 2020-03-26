package com.gravity.goose

import java.sql.Date

import org.apache.commons.lang.StringUtils

import scala.util.Try

package object spark {

  val ANNOTATOR_TITLE = "title"
  val ANNOTATOR_CONTENT = "content"
  val ANNOTATOR_DESCRIPTION = "description"
  val ANNOTATOR_KEYWORDS = "keywords"
  val ANNOTATOR_PUBLISH_DATE = "publishDate"

  // List of supported annotators
  val ANNOTATORS = Array(
    ANNOTATOR_TITLE,
    ANNOTATOR_CONTENT,
    ANNOTATOR_DESCRIPTION,
    ANNOTATOR_KEYWORDS,
    ANNOTATOR_PUBLISH_DATE
  )

  def scrapeArticles(it: Iterator[String], goose: Goose): Iterator[GooseArticle] = {
    it.map(url => {
      Try {
        val article = goose.extractContent(url)
        GooseArticle(
          url = url,
          title = if(StringUtils.isNotEmpty(article.title)) Some(article.title) else None,
          content = if(StringUtils.isNotEmpty(article.cleanedArticleText)) Some(article.cleanedArticleText.replaceAll("\\n+", "\n")) else None,
          description = if(StringUtils.isNotEmpty(article.metaDescription)) Some(article.metaDescription) else None,
          keywords = if(StringUtils.isNotEmpty(article.metaKeywords)) article.metaKeywords.split(",").map(_.trim.toUpperCase) else Array.empty[String],
          publishDate = if(article.publishDate != null) Some(new Date(article.publishDate.getTime)) else None,
          image = None
        )
      } getOrElse GooseArticle(url)
    })
  }

  case class GooseArticle(
                         url: String,
                         title: Option[String] = None,
                         content: Option[String] = None,
                         description: Option[String] = None,
                         keywords: Array[String] = Array.empty[String],
                         publishDate: Option[Date] = None,
                         image: Option[String] = None
                         )
}
