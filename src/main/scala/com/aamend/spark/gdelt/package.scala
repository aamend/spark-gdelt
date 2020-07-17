package com.aamend.spark

import java.sql.{Date, Timestamp}

import com.aamend.spark.gdelt.reference.{CameoCodes, CountryCodes, GcamCodes}
import com.gravity.goose.Goose
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql._

import scala.util.Try

package object gdelt {

  def T[A](r: () => A): Option[A] = {
    try {
      val x: A = r.apply()
      x match {
        case _ if(x == null) => None
        case p: String => if (p.trim.length == 0) None else Some(p.trim.asInstanceOf[A])
        case _ => Some(x)
      }
    } catch {
      case _: Throwable => None
    }
  }

  val ANNOTATOR_TITLE = "title"
  val ANNOTATOR_CONTENT = "content"
  val ANNOTATOR_DESCRIPTION = "description"
  val ANNOTATOR_KEYWORDS = "keywords"
  val ANNOTATOR_PUBLISH_DATE = "publishDate"
  val ANNOTATOR_IMAGE_URL = "imageURL"
  val ANNOTATOR_IMAGE_BASE64 = "imageBase64"

  // List of supported annotators
  val ANNOTATORS = Array(
    ANNOTATOR_TITLE,
    ANNOTATOR_CONTENT,
    ANNOTATOR_DESCRIPTION,
    ANNOTATOR_KEYWORDS,
    ANNOTATOR_PUBLISH_DATE,
    ANNOTATOR_IMAGE_URL,
    ANNOTATOR_IMAGE_BASE64
  )

  def scrapeContent(it: Iterator[String], goose: Goose): Iterator[Content] = {
    it.map(url => {
      Try {
        val article = goose.extractContent(url)
        Content(
          url = url,
          title = if (StringUtils.isNotEmpty(article.title)) Some(article.title) else None,
          content = if (StringUtils.isNotEmpty(article.cleanedArticleText)) Some(article.cleanedArticleText.replaceAll("\\n+", "\n")) else None,
          description = if (StringUtils.isNotEmpty(article.metaDescription)) Some(article.metaDescription) else None,
          keywords = if (StringUtils.isNotEmpty(article.metaKeywords)) article.metaKeywords.split(",").map(_.trim.toUpperCase) else Array.empty[String],
          publishDate = if (article.publishDate != null) Some(new Date(article.publishDate.getTime)) else None,
          imageURL = if (article.topImage != null && StringUtils.isNotEmpty(article.topImage.imageSrc)) Some(article.topImage.imageSrc) else None,
          imageBase64 = if (article.topImage != null && StringUtils.isNotEmpty(article.topImage.imageBase64)) Some(article.topImage.imageBase64) else None
        )
      } getOrElse Content(url)
    })
  }

  /**
    *
    * @param gkgRecordId
    * @param publishDate
    * @param sourceCollectionIdentifier
    * @param sourceCommonName
    * @param documentIdentifier
    * @param counts
    * @param enhancedCounts
    * @param themes
    * @param enhancedThemes
    * @param locations
    * @param enhancedLocations
    * @param persons
    * @param enhancedPersons
    * @param organisations
    * @param enhancedOrganisations
    * @param tone
    * @param enhancedDates
    * @param gcams
    * @param sharingImage
    * @param relatedImages
    * @param socialImageEmbeds
    * @param socialVideoEmbeds
    * @param quotations
    * @param allNames
    * @param amounts
    * @param translationInfo
    * @param extrasXML
    * @param hash
    * @param errors
    */
  case class GKGEvent(
                       gkgRecordId: Option[GkgRecordId] = None,
                       publishDate: Option[Timestamp] = None,
                       sourceCollectionIdentifier: Option[String] = None,
                       sourceCommonName: Option[String] = None,
                       documentIdentifier: Option[String] = None,
                       counts: List[Count] = List.empty[Count],
                       enhancedCounts: List[EnhancedCount] = List.empty[EnhancedCount],
                       themes: List[String] = List.empty[String],
                       enhancedThemes: List[EnhancedTheme] = List.empty[EnhancedTheme],
                       locations: List[Location] = List.empty[Location],
                       enhancedLocations: List[EnhancedLocation] = List.empty[EnhancedLocation],
                       persons: List[String] = List.empty[String],
                       enhancedPersons: List[EnhancedPerson] = List.empty[EnhancedPerson],
                       organisations: List[String] = List.empty[String],
                       enhancedOrganisations: List[EnhancedOrganisation] = List.empty[EnhancedOrganisation],
                       tone: Option[Tone] = None,
                       enhancedDates: List[EnhancedDate] = List.empty[EnhancedDate],
                       gcams: List[Gcam] = List.empty[Gcam],
                       sharingImage: Option[String] = None,
                       relatedImages: List[String] = List.empty[String],
                       socialImageEmbeds: List[String] = List.empty[String],
                       socialVideoEmbeds: List[String] = List.empty[String],
                       quotations: List[Quotation] = List.empty[Quotation],
                       allNames: List[Name] = List.empty[Name],
                       amounts: List[Amount] = List.empty[Amount],
                       translationInfo: Option[TranslationInfo] = None,
                       extrasXML: Option[String] = None,
                       hash: Option[String] = None,
                       errors: Option[String] = None
                     )

  case class GKGEventV1(
                       publishDate: Option[Timestamp] = None,
                       numArticles: Option[Int] = None,
                       counts: List[Count] = List.empty[Count],
                       themes: List[String] = List.empty[String],
                       locations: List[Location] = List.empty[Location],
                       persons: List[String] = List.empty[String],
                       organisations: List[String] = List.empty[String],
                       tone: Option[Tone] = None,
                       eventIds: List[Int] = List.empty[Int],
                       sources: List[String] = List.empty[String],
                       sourceUrls: List[String] = List.empty[String],
                       hash: Option[String] = None,
                       errors: Option[String] = None
  )

    case class GKGCountV1(
                         publishDate: Option[Timestamp] = None,
                         numArticles: Option[Int] = None,
                         counts: Option[Count] = None,
                         eventIds: List[Int] = List.empty[Int],
                         sources: List[String] = List.empty[String],
                         sourceUrls: List[String] = List.empty[String]
  )

  /**
    *
    * @param eventId            Globally unique identifier assigned to each event record that uniquely identifies it in the master dataset.
    *                           There is a large gap in the sequence between February 18, 2015 and February 19, 2015 with the switchover to GDELT 2.0
    * @param eventDay           Date the event took place
    * @param actor1Code
    * @param actor2Code
    * @param isRoot             The system codes every event found in an entire document, using an array of techniques to deference and link information together.
    *                           A number of previous projects such as the ICEWS initiative have found that events occurring in the lead paragraph of a document tend to be the most "important"
    *                           This flag can therefore be used as a proxy for the rough importance of an event to create subsets of the event stream.
    * @param cameoEventCode     This is the raw CAMEO action code describing the action that Actor1 performed upon Actor2.
    * @param cameoEventBaseCode CAMEO event codes are defined in a three-level taxonomy.
    *                           For events at level three in the taxonomy, this yields its level two leaf root node.
    *                           For example, code "0251" ("Appeal for easing of administrative sanctions") would yield an EventBaseCode of "025" ("Appeal to yield").
    *                           This makes it possible to aggregate events at various resolutions of specificity.
    *                           For events at levels two or one, this field will be set to EventCode.
    * @param cameoEventRootCode Similar to EventBaseCode, this defines the root-level category the event code falls under.
    *                           For example, code "0251" ("Appeal for easing of administrative sanctions") has a root code of "02" ("Appeal").
    *                           This makes it possible to aggregate events at various resolutions of specificity.
    *                           For events at levels two or one, this field will be set to EventCode.
    * @param quadClass          The entire CAMEO event taxonomy is ultimately organized under four primary classifications:
    *                           Verbal Cooperation, Material Cooperation, Verbal Conflict, and Material Conflict.
    *                           This field specifies this primary classification for the event type, allowing analysis at the highest level of aggregation.
    * @param goldstein          Each CAMEO event code is assigned a numeric score from -10 to +10, capturing the theoretical potential impact that type of event will have on the stability of a country.
    *                           This is known as the Goldstein Scale.
    *                           This field specifies the Goldstein score for each event type.
    *                           NOTE: this score is based on the type of event, not the specifics of the actual event record being recorded – thus two riots, one with 10 people and one with 10,000, will both receive the same Goldstein score.
    *                           This can be aggregated to various levels of time resolution to yield an approximation of the stability of a location over time.
    * @param numMentions        This is the total number of mentions of this event across all source documents during the 15 minute update in which it was first seen.
    *                           Multiple references to an event within a single document also contribute to this count.
    *                           This can be used as a method of assessing the "importance" of an event: the more discussion of that event, the more likely it is to be significant.
    *                           The total universe of source documents and the density of events within them vary over time, so it is recommended that this field be normalized by the average or other measure of the universe of events during the time period of interest.
    *                           This field is actually a composite score of the total number of raw mentions and the number of mentions extracted from reprocessed versions of each article (see the discussion for the Mentions table).
    * @param numSources         This is the total number of information sources containing one or more mentions of this event during the 15 minute update in which it was first seen.
    *                           This can be used as a method of assessing the "importance" of an event: the more discussion of that event, the more likely it is to be significant.
    *                           The total universe of sources varies over time, so it is recommended that this field be normalized by the average or other measure of the universe of events during the time period of interest.
    * @param numArticles        This is the total number of source documents containing one or more mentions of this event during the 15 minute update in which it was first seen.
    *                           This can be used as a method of assessing the “importance” of an event: the more discussion of that event, the more likely it is to be significant.
    *                           The total universe of source documents varies over time, so it is recommended that this field be normalized by the average or other measure of the universe of events during the time period of interest.
    * @param avgTone            This is the average “tone” of all documents containing one or more mentions of this event during the 15 minute update in which it was first seen.
    *                           The score ranges from -100 (extremely negative) to +100 (extremely positive).
    *                           Common values range between -10 and +10, with 0 indicating neutral.
    *                           This can be used as a method of filtering the “context” of events as a subtle measure of the importance of an event and as a proxy for the “impact” of that event.
    *                           For example, a riot event with a slightly negative average tone is likely to have been a minor occurrence, whereas if it had an extremely negative average tone, it suggests a far more serious occurrence.
    *                           A riot with a positive score likely suggests a very minor occurrence described in the context of a more positive narrative (such as a report of an attack occurring in a discussion of improving conditions on the ground in a country and how the number of attacks per day has been greatly reduced).
    * @param actor1Geo
    * @param actor2Geo
    * @param eventGeo
    * @param dateAdded          This field stores the date the event was added to the master database.
    * @param sourceUrl          This field records the URL or citation of the first news report it found this event in.
    *                           In most cases this is the first report it saw the article in, but due to the timing and flow of news reports through the processing pipeline, this may not always be the very first report, but is at least in the first few reports.
    * @param hash               This field is a hash digest of the Event input string
    * @param errors             This field will hold any parsing errors (TODO perhaps via https://typelevel.org/cats/datatypes/validated.html) 
    */
  case class Event(
                    eventId: Option[Int] = None,
                    eventDay: Option[Date] = None,
                    actor1Code: Option[Actor] = None,
                    actor2Code: Option[Actor] = None,
                    isRoot: Option[Boolean] = None,
                    cameoEventCode: Option[String] = None,
                    cameoEventBaseCode: Option[String] = None,
                    cameoEventRootCode: Option[String] = None,
                    quadClass: Option[String] = None,
                    goldstein: Option[Float] = None,
                    numMentions: Option[Int] = None,
                    numSources: Option[Int] = None,
                    numArticles: Option[Int] = None,
                    avgTone: Option[Float] = None,
                    actor1Geo: Option[Location] = None,
                    actor2Geo: Option[Location] = None,
                    eventGeo: Option[Location] = None,
                    dateAdded: Option[Timestamp] = None,
                    sourceUrl: Option[String] = None,
                    hash: Option[String] = None,
                    errors: Option[String] = None
                  )

    case class EventV1(
                    eventId: Option[Int] = None,
                    eventDay: Option[Date] = None,
                    actor1Code: Option[Actor] = None,
                    actor2Code: Option[Actor] = None,
                    isRoot: Option[Boolean] = None,
                    cameoEventCode: Option[String] = None,
                    cameoEventBaseCode: Option[String] = None,
                    cameoEventRootCode: Option[String] = None,
                    quadClass: Option[String] = None,
                    goldstein: Option[Float] = None,
                    numMentions: Option[Int] = None,
                    numSources: Option[Int] = None,
                    numArticles: Option[Int] = None,
                    avgTone: Option[Float] = None,
                    actor1Geo: Option[Location] = None,
                    actor2Geo: Option[Location] = None,
                    eventGeo: Option[Location] = None,
                    dateAdded: Option[Date] = None,
                    sourceUrl: Option[String] = None,
                    hash: Option[String] = None,
                    errors: Option[String] = None
                  )

  /**
    *
    * @param eventId           This is the ID of the event that was mentioned in the article.
    * @param eventTime         This is the 15-minute timestamp when the event being mentioned was first recorded by GDELT.
    *                          This field can be compared against the next one to identify events being mentioned for the first time (their first mentions) or to identify events of a particular vintage being mentioned now.
    * @param mentionTime       This is the 15-minute timestamp of the current update. This is identical for all entries in the update file but is included to make it easier to load the Mentions table into a database.
    * @param mentionType       This is a numeric identifier that refers to the source collection the document came from and is used to interpret the MentionIdentifier in the next column.
    *                          In essence, it specifies how to interpret the MentionIdentifier to locate the actual document.
    * @param mentionSourceName This is a human-friendly identifier of the source of the document.
    *                          For material originating from the open web with a URL this field will contain the top-level domain the page was from.
    *                          For BBC Monitoring material it will contain "BBC Monitoring" and for JSTOR material it will contain "JSTOR"
    *                          This field is intended for human display of major sources as well as for network analysis of information flows by source, obviating the requirement to perform domain or other parsing of the MentionIdentifier field.
    * @param mentionIdentifier This is the unique external identifier for the source document.
    *                          It can be used to uniquely identify the document and access it if you have the necessary subscriptions or authorizations and/or the document is public access.
    *                          This field can contain a range of values, from URLs of open web resources to textual citations of print or broadcast material to DOI identifiers for various document repositories.
    * @param sentenceId        The sentence within the article where the event was mentioned (starting with the first sentence as 1, the second sentence as 2, the third sentence as 3, and so on).
    *                          This can be used similarly to the CharOffset fields below, but reports the event’s location in the article in terms of sentences instead of characters, which is more amenable to certain measures of the "importance" of an event’s positioning within an article.
    * @param actor1CharOffset  The location within the article (in terms of English characters) where Actor1 was found.
    *                          This can be used in combination with the GKG or other analysis to identify further characteristics and attributes of the actor.
    * @param actor2CharOffset  The location within the article (in terms of English characters) where Actor2 was found.
    *                          This can be used in combination with the GKG or other analysis to identify further characteristics and attributes of the actor.
    * @param actionCharOffset  The location within the article (in terms of English characters) where the core Action description was found.
    *                          This can be used in combination with the GKG or other analysis to identify further characteristics and attributes of the actor.
    * @param inRawText         This records whether the event was found in the original unaltered raw article text (a value of 1) or whether advanced natural language processing algorithms were required to synthesize and rewrite the article text to identify the event (a value of 0).
    *                          See the discussion on the Confidence field below for more details.
    *                          Mentions with a value of “1” in this field likely represent strong detail-rich references to an event.
    * @param confidence        Percent confidence in the extraction of this event from this article.
    *                          See the discussion in the codebook at http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
    * @param mentionDocLen     The length in English characters of the source document (making it possible to filter for short articles focusing on a particular event versus long summary articles that casually mention an event in passing).
    * @param mentionDocTone    The same contents as the AvgTone field in the Events table, but computed for this particular article.
    * @param hash               This field is a hash digest of the Event input string
    * @param errors             This field will hold any parsing errors (TODO perhaps via https://typelevel.org/cats/datatypes/validated.html) 
    */
  case class Mention(
                      eventId: Option[Long] = None,
                      eventTime: Option[Timestamp] = None,
                      mentionTime: Option[Timestamp] = None,
                      mentionType: Option[String] = None,
                      mentionSourceName: Option[String] = None,
                      mentionIdentifier: Option[String] = None,
                      sentenceId: Option[Int] = None,
                      actor1CharOffset: Option[Int] = None,
                      actor2CharOffset: Option[Int] = None,
                      actionCharOffset: Option[Int] = None,
                      inRawText: Option[Int] = None,
                      confidence: Option[Int] = None,
                      mentionDocLen: Option[Int] = None,
                      mentionDocTone: Option[Float] = None,
                      hash: Option[String] = None,
                      errors: Option[String] = None
                    )

  /**
    *
    * @param latitude  This is the centroid latitude of the landmark for mapping.
    * @param longitude This is the centroid longitude of the landmark for mapping.
    */
  case class GeoPoint(
                       latitude: Option[Float] = None,
                       longitude: Option[Float] = None
                     )

  /**
    *
    * @param geoType     This field specifies the geographic resolution of the match type and holds one of the following values:
    *                    This can be used to filter events by geographic specificity, for example, extracting only those events with a landmark-level geographic resolution for mapping.
    * @param geoName     This is the full human-readable name of the matched location.
    *                    In the case of a country it is simply the country name.
    *                    For US and World states it is in the format of "State, Country Name", while for all other matches it is in the format of "City/Landmark, State, Country".
    *                    This can be used to label locations when placing events on a map.
    * @param countryCode This is the 2-character FIPS10-4 country code for the location.
    * @param adm1Code    This is the 2-character FIPS10-4 country code followed by the 2-character FIPS10-4 administrative division 1 (ADM1) code for the administrative division housing the landmark.
    *                    In the case of the United States, this is the 2-character shortform of the state’s name (such as "TX" for Texas).
    * @param adm2Code    For international locations this is the numeric Global Administrative Unit Layers (GAUL) administrative division 2 (ADM2) code assigned to each global location, while for US locations this is the two-character shortform of the state’s name (such as "TX" for Texas) followed by the 3-digit numeric county code (following the INCITS 31:200x standard used in GNIS).
    * @param geoPoint    The geo coordinates
    * @param featureId   This is the GNS or GNIS FeatureID for this location.
    *                    More information on these values can be found in Leetaru (2012).
    */
  case class Location(
                       geoType: Option[String] = None,
                       geoName: Option[String] = None,
                       countryCode: Option[String] = None,
                       adm1Code: Option[String] = None,
                       adm2Code: Option[String] = None,
                       geoPoint: Option[GeoPoint] = None,
                       featureId: Option[String] = None
                     )

  /**
    *
    * @param cameoRaw           The complete raw CAMEO code for Actor (includes geographic, class, ethnic, religious, and type classes).
    *                           May be blank if the system was unable to identify an Actor.
    * @param cameoName          The actual name of the Actor.
    *                           In the case of a political leader or organization, this will be the leader’s formal name (GEORGE W BUSH, UNITED NATIONS), for a geographic match it will be either the country or capital/major city name (UNITED STATES / PARIS), and for ethnic, religious, and type matches it will reflect the root match class (KURD, CATHOLIC, POLICE OFFICER, etc).
    *                           May be blank if the system was unable to identify an Actor.
    * @param cameoCountryCode   The 3-character CAMEO code for the country affiliation of Actor.
    *                           May be blank if the system was unable to identify an Actor1 or determine its country affiliation (such as "UNIDENTIFIED GUNMEN").
    * @param cameoGroupCode     If Actor is a known IGO/NGO/rebel organization (United Nations, World Bank, al-Qaeda, etc) with its own CAMEO code, this field will contain that code.
    * @param cameoEthnicCode    If the source document specifies the ethnic affiliation of Actor and that ethnic group has a CAMEO entry, the CAMEO code is entered here.
    * @param cameoReligion1Code If the source document specifies the religious affiliation of Actor and that religious group has a CAMEO entry, the CAMEO code is entered here.
    * @param cameoReligion2Code If multiple religious codes are specified for Actor, this contains the secondary code.
    *                           Some religion entries automatically use two codes, such as Catholic, which invokes Christianity as Code1 and Catholicism as Code2.
    * @param cameoType1Code     The 3-character CAMEO code of the CAMEO "type" or "role" of Actor, if specified.
    *                           This can be a specific role such as Police Forces, Government, Military, Political Opposition, Rebels, etc, a broad role class such as Education, Elites, Media, Refugees, or organizational classes like Non-Governmental Movement.
    *                           Special codes such as Moderate and Radical may refer to the operational strategy of a group.
    * @param cameoType2Code     If multiple type/role codes are specified for Actor1, this returns the second code.
    * @param cameoType3Code     If multiple type/role codes are specified for Actor1, this returns the third code.
    */
  case class Actor(
                    cameoRaw: Option[String] = None,
                    cameoName: Option[String] = None,
                    cameoCountryCode: Option[String] = None,
                    cameoGroupCode: Option[String] = None,
                    cameoEthnicCode: Option[String] = None,
                    cameoReligion1Code: Option[String] = None,
                    cameoReligion2Code: Option[String] = None,
                    cameoType1Code: Option[String] = None,
                    cameoType2Code: Option[String] = None,
                    cameoType3Code: Option[String] = None
                  )

  case class Count(
                    countType: Option[String] = None,
                    count: Option[Long] = None,
                    objectType: Option[String] = None,
                    location: Option[Location] = None
                  )

  case class Tone(
                   tone: Option[Float] = None,
                   positiveScore: Option[Float] = None,
                   negativeScore: Option[Float] = None,
                   polarity: Option[Float] = None,
                   activityReferenceDensity: Option[Float] = None,
                   selfGroupReferenceDensity: Option[Float] = None,
                   wordCount: Option[Int] = None
                 )

  case class EnhancedLocation(
                               location: Option[Location] = None,
                               charOffset: Option[Int] = None
                             )

  case class EnhancedTheme(
                            theme: Option[String] = None,
                            charOffset: Option[Int] = None
                          )

  case class EnhancedPerson(
                             person: Option[String] = None,
                             charOffset: Option[Int] = None
                           )

  case class EnhancedOrganisation(
                                   organisation: Option[String] = None,
                                   charOffset: Option[Int] = None
                                 )

  case class Gcam(
                   gcamCode: Option[String] = None,
                   gcamValue: Option[Double] = None
                 )

  case class GkgRecordId(
                          publishDate: Option[Timestamp] = None,
                          translingual: Option[Boolean] = None,
                          numberInBatch: Option[Int] = None
                        )

  case class EnhancedDate(
                           dateResolution: Option[Int] = None,
                           month: Option[Int] = None,
                           day: Option[Int] = None,
                           year: Option[Int] = None,
                           charOffset: Option[Int] = None
                         )

  case class EnhancedCount(
                            count: Option[Count] = None,
                            charOffset: Option[Int] = None
                          )

  implicit class GdeltSpark(dfReader: DataFrameReader) {

    def gdeltGkg(inputPaths: String*): Dataset[GKGEvent] = { //, version: String): Dataset[Row] = {
      /* val ds = version match {
        case "V1" => dfReader.option("header",true).textFile(inputPaths:_*)
        case "V2" => dfReader.textFile(inputPaths:_*)
        case "Counts" => 
        case _ => throw new IllegalArgumentException("version can only be 'V1', 'V2' or 'Counts'.")
      } */
      val ds = dfReader.textFile(inputPaths:_*)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseGkg)
      /* version match {
        case "V1" => ds.map(GdeltParser.parseGkgV1)
        case "V2" => ds.map(GdeltParser.parseGKG)
        case "Counts" => ds.map(GdeltParser.parseGKGCounts)
        case _ => throw new IllegalArgumentException("version can only be 'V1', 'V2' or 'Counts'.")
      } */
    }

    def gdeltGkg(inputPath: String): Dataset[GKGEvent] = {
     gdeltGkg(Seq(inputPath): _*)
    }

    def gdeltGkgV1(inputPaths: String*): Dataset[GKGEventV1] = {
      val ds = dfReader.option("header",true).textFile(inputPaths:_*)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseGkgV1)
    }

    def gdeltGkgV1(inputPath: String): Dataset[GKGEventV1] = {
     gdeltGkgV1(Seq(inputPath): _*)
    }

     def gdeltGkgCountV1(inputPaths: String*): Dataset[GKGCountV1] = {
      val ds = dfReader.option("header",true).textFile(inputPaths:_*)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseGkgCountV1)
    }

    def gdeltGkgCountV1(inputPath: String): Dataset[GKGCountV1] = {
     gdeltGkgCountV1(Seq(inputPath): _*)
    }


    def gdeltEvent(inputPaths: String*): Dataset[Event] = {
      val ds = dfReader.textFile(inputPaths:_*)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseEvent)
    }

    def gdeltEvent(inputPath: String): Dataset[Event] = {
      gdeltEvent(Seq(inputPath): _*)
    }

    def gdeltEventV1(inputPaths: String*): Dataset[EventV1] = {
      val ds = dfReader.textFile(inputPaths:_*)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseEventV1)
    }
    
    def gdeltEventV1(inputPath: String): Dataset[EventV1] = {
      gdeltEventV1(Seq(inputPath): _*)
    }

    def gdeltMention(inputPaths: String*): Dataset[Mention] = {
      val ds = dfReader.textFile(inputPaths:_*)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseMention)
    }

    def gdeltMention(inputPath: String): Dataset[Mention] = {
      gdeltMention(Seq(inputPath): _*)
    }

  }

  implicit class GdeltReferenceData(spark: SparkSession) {

    def loadCountryCodes: Dataset[CountryCode] = {
      CountryCodes.load(spark)
    }

    def loadGcams: Dataset[GcamCode] = {
      GcamCodes.load(spark)
    }

    def loadCameoEventCodes: Dataset[CameoCode] = {
      CameoCodes.loadEventCode(spark)
    }

    def loadCameoTypeCodes: Dataset[CameoCode] = {
      CameoCodes.loadTypeCode(spark)
    }

    def loadCameoGroupCodes: Dataset[CameoCode] = {
      CameoCodes.loadGroupCode(spark)
    }

    def loadCameoEthnicCodes: Dataset[CameoCode] = {
      CameoCodes.loadEthnicCode(spark)
    }

    def loadCameoReligionCodes: Dataset[CameoCode] = {
      CameoCodes.loadReligionCode(spark)
    }

    def loadCameoCountryCodes: Dataset[CameoCode] = {
      CameoCodes.loadCountryCode(spark)
    }
  }

  case class Quotation(
                        charLength: Option[Int] = None,
                        verb: Option[String] = None,
                        quote: Option[String] = None,
                        charOffset: Option[Int] = None
                      )

  case class Name(
                   name: Option[String] = None,
                   charOffset: Option[Int] = None
                 )

  case class Amount(
                     amount: Option[Double] = None,
                     amountType: Option[String] = None,
                     charOffset: Option[Int] = None
                   )

  case class TranslationInfo(
                              SRCLC: Option[String] = None,
                              ENG: Option[String] = None
                            )

  case class CountryCode(
                          iso: Option[String] = None,
                          iso3: Option[String] = None,
                          isoNumeric: Option[String] = None,
                          fips: Option[String] = None,
                          country: Option[String] = None
                        )

  case class CameoCode(
                        cameoCode: Option[String] = None,
                        cameoValue: Option[String] = None
                      )

  case class GcamCode(
                       gcamCode: Option[String] = None,
                       dictionaryId: Option[String] = None,
                       dimensionId: Option[String] = None,
                       dictionaryType: Option[String] = None,
                       languageCode: Option[String] = None,
                       dictionaryHumanName: Option[String] = None,
                       dimensionHumanName: Option[String] = None,
                       dictionaryCitation: Option[String] = None
                     )

   case class Content(
                      url: String,
                      title: Option[String] = None,
                      content: Option[String] = None,
                      description: Option[String] = None,
                      keywords: Array[String] = Array.empty[String],
                      publishDate: Option[Date] = None,
                      imageURL: Option[String] = None,
                      imageBase64: Option[String] = None
                    )

}
