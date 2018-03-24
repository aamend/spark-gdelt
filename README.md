# Gdelt Spark

[![Build Status](https://travis-ci.org/aamend/gdelt-spark.svg?branch=master)](https://travis-ci.org/aamend/gdelt-spark) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aamend.spark/spark-gdelt/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.aamend.spark.spark-gdelt)

## GDELT (global database of events language and tone)

![gdelt-spark](/images/gdelt.png)

[GDELT Project](https://www.gdeltproject.org/): 
*The GDELT project monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages and identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day, creating a free open platform for computing on the entire world.*

The GDELT universe being quite large, its data format is by essence complex and convoluted. Official documentation can be found here:
* [http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf)
* [http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook.pdf](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook.pdf)
* [http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf](http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf)

## gdelt-spark

This project has been built to make [GDELT v2](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/) environment easy to load on a Spark based environment. 
While the `1.0` version comes with a GDELT data model (case classes), a set of parsers and all its reference data, it is planned to enrich this library with advance analytics and libraries I gathered / created over the past few years. 
I also plan to provide GDELT near realtime support as Spark structured streaming. 

### Getting Started

_spark-gdelt_ project has been built for __Scala 2.11__ and __Spark 2.1.0__ and is available on Maven central.

```xml
<dependency>
  <groupId>com.aamend.spark</groupId>
  <artifactId>spark-gdelt</artifactId>
  <version>x.y.z</version>
</dependency>
```

Also available as a [spark package](https://spark-packages.org/package/aamend/spark-gdelt), include this package in your Spark Applications as follows

```bash
> $SPARK_HOME/bin/spark-shell --packages com.aamend.spark:spark-gdelt:x.y.z
```

### Usage

### GDELT core feeds
Loading core GDELT files (unzipped) as `Dataset[_]`. Note we support both english and translingual files, V2 only. 

```scala
import com.aamend.spark.gdelt._

val gdeltEventDS: Dataset[Event] = spark.read.gdeltEvent("/path/to/event.csv")
val gdeltGkgDS: Dataset[GKGEvent] = spark.read.gdeltGkg("/path/to/gkg.csv")
val gdeltMention: Dataset[Mention] = spark.read.gdeltMention("/path/to/mention.csv")
```

Here is an example of `Dataset[Event]`

```
+---------+----------+--------------------+--------------------+------+--------------+------------------+------------------+--------------------+---------+-----------+----------+-----------+-----------+--------------------+--------------------+--------------------+----------+--------------------+
|  eventId|  eventDay|          actor1Code|          actor2Code|isRoot|cameoEventCode|cameoEventBaseCode|cameoEventRootCode|           quadClass|goldstein|numMentions|numSources|numArticles|    avgTone|           actor1Geo|           actor2Geo|            eventGeo| dateAdded|           sourceUrl|
+---------+----------+--------------------+--------------------+------+--------------+------------------+------------------+--------------------+---------+-----------+----------+-----------+-----------+--------------------+--------------------+--------------------+----------+--------------------+
|741462369|2008-03-25|[USA,UNITED STATE...|         [,,,,,,,,,]| false|           050|               050|                05|  VERBAL_COOPERATION|      3.5|          4|         1|          4|  1.1655011|[COUNTRY,United S...|[UNKNOWN,,,,,[NaN...|[COUNTRY,United S...|2018-03-23|https://www.citiz...|
|741462370|2017-03-23|         [,,,,,,,,,]|[USA,CALIFORNIA,U...|  true|           100|               100|                10|     VERBAL_CONFLICT|     -5.0|          3|         1|          3| -1.2836186|[UNKNOWN,,,,,[NaN...|[USCITY,San Franc...|[USCITY,San Franc...|2018-03-23|https://www.nytim...|
|741462371|2017-03-23|         [,,,,,,,,,]|[USACVL,NORTH CAR...|  true|           043|               043|                04|  VERBAL_COOPERATION|      2.8|          8|         1|          8|  0.9041591|[UNKNOWN,,,,,[NaN...|[USSTATE,North Ca...|[USSTATE,North Ca...|2018-03-23|http://www.charlo...|
|741462372|2017-03-23|[EDU,INTELLECTUAL...|[CHN,CHINA,CHN,,,...|  true|           111|               111|                11|     VERBAL_CONFLICT|     -2.0|         10|         1|         10| -2.2959185|[WORLDCITY,Guangz...|[WORLDCITY,Guangz...|[WORLDCITY,Guangz...|2018-03-23|https://www.teleg...|
|741462373|2017-03-23|[GOV,GOVERNMENT,,...|         [,,,,,,,,,]| false|           014|               014|                01|  VERBAL_COOPERATION|      0.0|         10|         1|         10|  2.5117738|[WORLDSTATE,Bay O...|[UNKNOWN,,,,,[NaN...|[WORLDSTATE,Bay O...|2018-03-23|http://www.nzhera...|
|741462374|2017-03-23|[USA,UNITED STATE...|         [,,,,,,,,,]| false|          0831|               083|                08|MATERIAL_COOPERATION|      5.0|          4|         1|          4|  -2.259887|[USCITY,Willow Cr...|[UNKNOWN,,,,,[NaN...|[USCITY,Willow Cr...|2018-03-23|https://www.relig...|
|741462375|2017-03-23|[USA,UNITED STATE...|[GOV,PRESIDENT,,,...|  true|           110|               110|                11|     VERBAL_CONFLICT|     -2.0|          1|         1|          1|-0.63897765|[COUNTRY,Russia,R...|[COUNTRY,Russia,R...|[COUNTRY,Russia,R...|2018-03-23|http://www.tribun...|
|741462376|2017-03-23|[USA,UNITED STATE...|[RUSGOV,RUSSIA,RU...|  true|           110|               110|                11|     VERBAL_CONFLICT|     -2.0|          1|         1|          1|-0.63897765|[COUNTRY,North Ko...|[COUNTRY,North Ko...|[COUNTRY,North Ko...|2018-03-23|http://www.tribun...|
|741462377|2017-03-23|[USA,NORTH CAROLI...|[USACVL,UNITED ST...|  true|           042|               042|                04|  VERBAL_COOPERATION|      1.9|          2|         1|          2|  0.9041591|[USCITY,Chapel Hi...|[USCITY,Chapel Hi...|[USCITY,Chapel Hi...|2018-03-23|http://www.charlo...|
|741462378|2017-03-23|[USACVL,NORTH CAR...|         [,,,,,,,,,]|  true|           042|               042|                04|  VERBAL_COOPERATION|      1.9|          8|         1|          8|  0.9041591|[USCITY,Chapel Hi...|[UNKNOWN,,,,,[NaN...|[USCITY,Chapel Hi...|2018-03-23|http://www.charlo...|
|741462379|2017-03-23|[USACVL,UNITED ST...|[USA,NORTH CAROLI...|  true|           043|               043|                04|  VERBAL_COOPERATION|      2.8|          2|         1|          2|  0.9041591|[USSTATE,North Ca...|[USSTATE,North Ca...|[USSTATE,North Ca...|2018-03-23|http://www.charlo...|
|741462380|2017-03-23|[USAGOV,UNITED ST...|[USAMED,UNITED ST...| false|           036|               036|                03|  VERBAL_COOPERATION|      4.0|          1|         1|          1|-0.75376886|[USCITY,White Hou...|[USCITY,White Hou...|[USCITY,White Hou...|2018-03-23|http://www.breitb...|
|741462381|2017-03-23|[chr,CHEROKEE,,,c...|         [,,,,,,,,,]|  true|           193|               193|                19|   MATERIAL_CONFLICT|    -10.0|          5|         1|          5| -1.4614646|[USSTATE,Michigan...|[UNKNOWN,,,,,[NaN...|[USSTATE,Michigan...|2018-03-23|https://jalopnik....|
|741462382|2018-02-21|[CAN,CANADA,CAN,,...|[VNM,VIETNAM,VNM,...| false|           043|               043|                04|  VERBAL_COOPERATION|      2.8|          2|         1|          2| -12.418301|[COUNTRY,Canada,C...|[COUNTRY,Canada,C...|[COUNTRY,Canada,C...|2018-03-23|http://calgarysun...|
|741462383|2018-02-21|[CAN,CANADA,CAN,,...|[VNM,VIETNAM,VNM,...| false|           043|               043|                04|  VERBAL_COOPERATION|      2.8|          8|         1|          8| -12.418301|[COUNTRY,Canada,C...|[COUNTRY,Vietnam ...|[COUNTRY,Canada,C...|2018-03-23|http://calgarysun...|
|741462384|2018-02-21|[DEU,GERMANY,DEU,...|[GOV,GOVERNMENT,,...| false|           036|               036|                03|  VERBAL_COOPERATION|      4.0|         14|         7|         14| -0.9155244|[WORLDCITY,Berlin...|[COUNTRY,Mali,ML,...|[WORLDCITY,Berlin...|2018-03-23|https://www.barri...|
|741462385|2018-02-21|[DEU,GERMANY,DEU,...|[GOV,GOVERNMENT,,...| false|           036|               036|                03|  VERBAL_COOPERATION|      4.0|          4|         2|          4|-0.18219218|[WORLDCITY,Berlin...|[COUNTRY,Mali,ML,...|[COUNTRY,Mali,ML,...|2018-03-23|http://lethbridge...|
|741462386|2018-02-21|[EDU,STUDENTS AND...|[UAF,GUNMAN,,,,,,...| false|           180|               180|                18|   MATERIAL_CONFLICT|     -9.0|          1|         1|          1| -1.7881706|[USCITY,Baltimore...|[USCITY,Baltimore...|[USCITY,Baltimore...|2018-03-23|http://www.watert...|
|741462387|2018-02-21|[GOV,GOVERNMENT,,...|[DEU,GERMANY,DEU,...| false|           036|               036|                03|  VERBAL_COOPERATION|      4.0|         14|         7|         14| -0.9155244|[COUNTRY,Mali,ML,...|[WORLDCITY,Berlin...|[WORLDCITY,Berlin...|2018-03-23|https://www.barri...|
|741462388|2018-02-21|[GOV,GOVERNMENT,,...|[DEU,GERMANY,DEU,...| false|           036|               036|                03|  VERBAL_COOPERATION|      4.0|          4|         2|          4|-0.18219218|[COUNTRY,Mali,ML,...|[WORLDCITY,Berlin...|[COUNTRY,Mali,ML,...|2018-03-23|http://lethbridge...|
+---------+----------+--------------------+--------------------+------+--------------+------------------+------------------+--------------------+---------+-----------+----------+-----------+-----------+--------------------+--------------------+--------------------+----------+--------------------+
```

#### GDELT reference data

```scala
import com.aamend.spark.gdelt._

val countryCodes: Dataset[CountryCode] = spark.loadCountryCodes
val gcam: Dataset[GcamCode] = spark.loadGcams
val cameoEvent: Dataset[CameoCode] = spark.loadCameoEventCodes
val cameoType: Dataset[CameoCode] = spark.loadCameoTypeCodes
val cameoGroup: Dataset[CameoCode] = spark.loadCameoGroupCodes
val cameoEthnic: Dataset[CameoCode] = spark.loadCameoEthnicCodes
val cameoReligion: Dataset[CameoCode] = spark.loadCameoReligionCodes
val cameoCountry: Dataset[CameoCode] = spark.loadCameoCountryCodes
```

Here is an example of `Dataset[CameoCode]`

```
+---------+--------------------+
|cameoCode|          cameoValue|
+---------+--------------------+
|      COP|       police forces|
|      GOV|          government|
|      INS|          insurgents|
|      JUD|           judiciary|
|      MIL|            military|
|      OPP|political opposition|
|      REB|              rebels|
|      SEP|   separatist rebels|
|      SPY|  state intelligence|
|      UAF|unaligned armed f...|
|      AGR|         agriculture|
|      BUS|            business|
|      CRM|            criminal|
|      CVL|            civilian|
|      DEV|         development|
|      EDU|           education|
|      ELI|              elites|
|      ENV|       environmental|
|      HLH|              health|
|      HRI|        human rights|
+---------+--------------------+
```

## Version

1.0

## Authors

Antoine Amend - [[antoine.amend@gmail.com]](antoine.amend@gmail.com)

## License

Apache License, version 2.0
