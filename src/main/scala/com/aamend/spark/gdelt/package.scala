package com.aamend.spark

import java.sql.{Date, Timestamp}

import com.aamend.spark.gdelt.reference.{CameoCodes, CountryCodes, GcamCodes}
import org.apache.spark.sql._

package object gdelt {

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
    */
  case class GKGEvent(
                       gkgRecordId: GkgRecordId = GkgRecordId(),
                       publishDate: Timestamp = new Timestamp(0L),
                       sourceCollectionIdentifier: String = "",
                       sourceCommonName: String = "",
                       documentIdentifier: String = "",
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
                       tone: Tone = Tone(),
                       enhancedDates: List[EnhancedDate] = List.empty[EnhancedDate],
                       gcams: List[Gcam] = List.empty[Gcam],
                       sharingImage: String = "",
                       relatedImages: List[String] = List.empty[String],
                       socialImageEmbeds: List[String] = List.empty[String],
                       socialVideoEmbeds: List[String] = List.empty[String],
                       quotations: List[Quotation] = List.empty[Quotation],
                       allNames: List[Name] = List.empty[Name],
                       amounts: List[Amount] = List.empty[Amount],
                       translationInfo: TranslationInfo = TranslationInfo(),
                       extrasXML: String = ""
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
    */
  case class Event(
                    eventId: Int = 0,
                    eventDay: Date = new Date(0L),
                    actor1Code: Actor = Actor(),
                    actor2Code: Actor = Actor(),
                    isRoot: Boolean = false,
                    cameoEventCode: String = "",
                    cameoEventBaseCode: String = "",
                    cameoEventRootCode: String = "",
                    quadClass: String = "",
                    goldstein: Float = 0.0f,
                    numMentions: Int = 0,
                    numSources: Int = 0,
                    numArticles: Int = 0,
                    avgTone: Float = 0.0f,
                    actor1Geo: Location = Location(),
                    actor2Geo: Location = Location(),
                    eventGeo: Location = Location(),
                    dateAdded: Date = new Date(0L),
                    sourceUrl: String = ""
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
    */
  case class Mention(
                      eventId: Long = 0L,
                      eventTime: Timestamp = new Timestamp(0L),
                      mentionTime: Timestamp = new Timestamp(0L),
                      mentionType: String = "",
                      mentionSourceName: String = "",
                      mentionIdentifier: String = "",
                      sentenceId: Int = 0,
                      actor1CharOffset: Int = 0,
                      actor2CharOffset: Int = 0,
                      actionCharOffset: Int = 0,
                      inRawText: Int = 0,
                      confidence: Int = 0,
                      mentionDocLen: Int = 0,
                      mentionDocTone: Float = 0.0f
                    )

  /**
    *
    * @param latitude  This is the centroid latitude of the landmark for mapping.
    * @param longitude This is the centroid longitude of the landmark for mapping.
    */
  case class GeoPoint(
                       latitude: Float = Float.NaN,
                       longitude: Float = Float.NaN
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
                       geoType: String = "",
                       geoName: String = "",
                       countryCode: String = "",
                       adm1Code: String = "",
                       adm2Code: String = "",
                       geoPoint: GeoPoint = GeoPoint(),
                       featureId: String = ""
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
                    cameoRaw: String = "",
                    cameoName: String = "",
                    cameoCountryCode: String = "",
                    cameoGroupCode: String = "",
                    cameoEthnicCode: String = "",
                    cameoReligion1Code: String = "",
                    cameoReligion2Code: String = "",
                    cameoType1Code: String = "",
                    cameoType2Code: String = "",
                    cameoType3Code: String = ""
                  )

  case class Count(
                    countType: String = "",
                    count: Long = 0,
                    objectType: String = "",
                    location: Location = Location()
                  )

  case class Tone(
                   tone: Float = 0.0f,
                   positiveScore: Float = 0.0f,
                   negativeScore: Float = 0.0f,
                   polarity: Float = 0.0f,
                   activityReferenceDensity: Float = 0.0f,
                   selfGroupReferenceDensity: Float = 0.0f,
                   wordCount: Int = 0
                 )

  case class EnhancedLocation(
                               location: Location = Location(),
                               charOffset: Int = 0
                             )

  case class EnhancedTheme(
                            theme: String = "",
                            charOffset: Int = 0
                          )

  case class EnhancedPerson(
                             person: String = "",
                             charOffset: Int = 0
                           )

  case class EnhancedOrganisation(
                                   organisation: String = "",
                                   charOffset: Int = 0
                                 )

  case class Gcam(
                   gcamCode: String = "",
                   gcamValue: Double = 0.0
                 )

  case class GkgRecordId(
                          publishDate: Timestamp = new Timestamp(0L),
                          translingual: Boolean = false,
                          numberInBatch: Int = 0
                        )

  case class EnhancedDate(
                           dateResolution: Int = 0,
                           month: Int = 0,
                           day: Int = 0,
                           year: Int = 0,
                           charOffset: Int = 0
                         )

  case class EnhancedCount(
                            count: Count = Count(),
                            charOffset: Int = 0
                          )

  case class Quotation(
                        charLength: Int = 0,
                        verb: String = "",
                        quote: String = "",
                        charOffset: Int = 0
                      )

  case class Name(
                   name: String = "",
                   charOffset: Int = 0
                 )

  case class Amount(
                     amount: Double = 0.0d,
                     amountType: String = "",
                     charOffset: Int = 0
                   )

  case class TranslationInfo(
                              SRCLC: String = "",
                              ENG: String = ""
                            )

  case class CountryCode(
                          iso: String = "",
                          iso3: String = "",
                          isoNumeric: String = "",
                          fips: String = "",
                          country: String = ""
                        )

  case class CameoCode(
                        cameoCode: String,
                        cameoValue: String
                      )

  case class GcamCode(
                       gcamCode: String = "",
                       dictionaryId: String = "",
                       dimensionId: String = "",
                       dictionaryType: String = "",
                       languageCode: String = "",
                       dictionaryHumanName: String = "",
                       dimensionHumanName: String = "",
                       dictionaryCitation: String = ""
                     )

  implicit class GdeltSpark(dfReader: DataFrameReader) {
    def gdeltGkg(inputDir: String): Dataset[GKGEvent] = {
      val ds = dfReader.textFile(inputDir)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseGkg)
    }

    def gdeltEvent(inputDir: String): Dataset[Event] = {
      val ds = dfReader.textFile(inputDir)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseEvent)
    }

    def gdeltMention(inputDir: String): Dataset[Mention] = {
      val ds = dfReader.textFile(inputDir)
      import ds.sparkSession.implicits._
      ds.map(GdeltParser.parseMention)
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

}
