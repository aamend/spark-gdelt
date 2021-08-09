# Gdelt Spark

[![Build Status](https://travis-ci.org/aamend/gdelt-spark.svg?branch=master)](https://travis-ci.org/aamend/spark-gdelt) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aamend.spark/spark-gdelt/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.aamend.spark.spark-gdelt)

## GDELT (global database of events language and tone)

![gdelt-spark](/images/gdelt.png)

[GDELT Project](https://www.gdeltproject.org/): 
*The GDELT project monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages and identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day, creating a free open platform for computing on the entire world.*

The GDELT universe being quite large, its data format is by essence complex and convoluted. Official documentation can be found here:
* V1:
  * [http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf)
  * [http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook.pdf](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook.pdf)
* V2:
  * [http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)
  * [http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)
* Cameo 
  * [http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf](http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf)
## gdelt-spark

This project has been built to make [GDELT v2](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/) environment easy to load on a Spark based environment. 
While the `v1.0` version only came with a GDELT data model (case classes), a set of parsers and all its reference data, `v2.0` embeds the [Goose](https://github.com/GravityLabs/goose/wiki) library packaged for `Scala 2.11`. 
It is planned to later enrich this library with advance analytics and libraries I gathered / created over the past few years. 

### Getting Started

_spark-gdelt_ project has been built for __Scala 2.11__ and __Spark 2.1.0__ and is available on Maven central.

```xml
<dependency>
  <groupId>com.aamend.spark</groupId>
  <artifactId>spark-gdelt</artifactId>
  <version>x.y.z</version>
</dependency>
```

Also available as a [spark package](https://spark-packages.org/package/aamend/gdelt-spark), include this package in your Spark Applications as follows

```bash
spark-shell --packages com.aamend.spark:spark-gdelt:x.y.z
```

### Usage

### GDELT core feeds
Loading core GDELT files (unzipped) as `Dataset[_]`. 
Note we support both english and translingual files.

```scala
import com.aamend.spark.gdelt._

val gdeltEventDS: Dataset[EventV1] = spark.read.gdeltEvent("/path/to/eventV1.csv")
val gdeltGkgDS: Dataset[GKGEventV1] = spark.read.gdeltGkg("/path/to/gkgV1.csv")
val gdeltGkgDS: Dataset[GKGCountV1] = spark.read.gdeltGkg("/path/to/gkgCountV1.csv")

val gdeltEventDS: Dataset[EventV2] = spark.read.gdeltEvent("/path/to/eventV2.csv")
val gdeltGkgDS: Dataset[GKGEventV2] = spark.read.gdeltGkg("/path/to/gkgV2.csv")
val gdeltMention: Dataset[MentionV2] = spark.read.gdeltMention("/path/to/mentions.csv")

val gdeltEventNormDaily: Dataset[EventNormDaily] = spark.read.gdeltMention("/path/to/normDaily.csv")
val EventNormDailyByCountry: Dataset[EventNormDailyByCountry] = spark.read.gdeltMention("/path/to/normDailyByCountry.csv")



```

Here is an example of `Dataset[EventV2]`

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
Example of `Dataset[GKGEventV2]`
```
+--------------------+-------------------+--------------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+---------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+--------------------+--------------------+--------------------+--------------------+---------------+---------+--------------------+------+
|         gkgRecordId|        publishDate|sourceCollectionIdentifier|    sourceCommonName|  documentIdentifier|              counts|      enhancedCounts|              themes|      enhancedThemes|           locations|   enhancedLocations|             persons|     enhancedPersons|       organisations|enhancedOrganisations|                tone|       enhancedDates|               gcams|        sharingImage|       relatedImages|socialImageEmbeds|   socialVideoEmbeds|          quotations|            allNames|             amounts|translationInfo|extrasXML|                hash|errors|
+--------------------+-------------------+--------------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+---------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+--------------------+--------------------+--------------------+--------------------+---------------+---------+--------------------+------+
|[2015-02-18 23:00...|2015-02-18 23:00:00|             CITATION_ONLY|      BBC Monitoring|as listed in Russ...|[[ARREST, 400, po...|[[[ARREST, 400, p...|[TERROR, REBELS, ...|[[TAX_FNCACT, 201...|[[WORLDCITY, Buda...|[[[WORLDCITY, Nov...|[petro poroshenko...|[[Petro Poroshenk...|           [gazprom]|     [[Gazprom, 666]]|[-5.347594, 2.139...|            [[,,,,]]|[[wc, 693.0], [c1...|                null|                  []|               []|                  []|[[134,, prisoners...|[[Channel One, 62...|[[3.0, channels, ...|            [,]|     null|ylh7PfjovqpPVR3gY...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|             CITATION_ONLY|      BBC Monitoring|Al-Sharq al-Awsat...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_FNCACT, TAX_...|[[IDEOLOGY, 6790]...|[[COUNTRY, Qatar,...|[[[COUNTRY, Turks...|[al-arab kobane, ...|[[Al-Arab Kobane,...|[development part...| [[Development Par...|[-0.5405405, 3.82...|            [[,,,,]]|[[wc, 2376.0], [c...|                null|                  []|               []|                  []|[[26,, greatly co...|[[Emrullah Isler,...|[[2.0, fronts at ...|            [,]|     null|sP7HQMYgpy8X4yqMy...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|            wjon.com|http://wjon.com/w...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[MANMADE_DISASTER...|[[MANMADE_DISASTE...|      [[,,,,, [,],]]|   [[[,,,,, [,],],]]|                  []|               [[,]]|                  []|                [[,]]|[0.0, 0.0, 0.0, 0...|            [[,,,,]]|[[wc, 93.0], [c12...|http://wac.450F.e...|[http:/wac.450F.e...|               []|[https://youtube....|             [[,,,]]|[[Waite Park, 125...|[[500.0, people w...|            [,]|     null|nzgTvMiJDx/iKuDsP...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|            wjol.com|http://www.wjol.c...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[LEADER, TAX_FNCA...|[[TAX_FNCACT, 131...|[[USSTATE, Wiscon...|[[[USSTATE, Wisco...|[michael madigan,...|[[Michael Madigan...|                  []|                [[,]]|[-4.587156, 1.834...|            [[,,,,]]|[[wc, 103.0], [c1...|                null|                  []|               []|                  []|             [[,,,]]|[[Governor Rauner...|              [[,,]]|            [,]|     null|xLG+HnyRAr2X+yvNq...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|    straitstimes.com|http://www.strait...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[DRONES, TAX_WORL...|[[TAX_FNCACT, 634...|[[COUNTRY, United...|[[[USCITY, Miami,...|    [omid farokhzad]|[[Omid Farokhzad,...|[columbia univers...| [[Columbia Univer...|[-1.8050542, 3.61...|[[4, 2, 18, 0, 706]]|[[wc, 255.0], [c1...|http://www.strait...|                  []|               []|                  []|             [[,,,]]|[[United States, ...|[[5.0, weeks, 1194]]|            [,]|     null|XUrqZvmafEU7Qctqs...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|      indiatimes.com|http://timesofind...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[DELAY, PROTEST, ...|[[MEDIA_MSM, 1293...|[[WORLDCITY, Back...|[[[WORLDCITY, Che...|                  []|               [[,]]|[institute of rem...| [[Institute Of Re...|[-2.846975, 2.135...|[[1, 0, 0, 2006, ...|[[wc, 265.0], [c1...|http://timesofind...|                  []|               []|                  []|             [[,,,]]|[[Brihanmumbai Mu...|[[9.0, months, 10...|            [,]|     null|YJGQJxmNtGCc+8PQF...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|   illinoisstate.edu|http://stories.il...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_WORLDLANGUAG...|[[TAX_FNCACT, 807...|[[USSTATE, Illino...|[[[USCITY, Shawne...| [jennifer holcombe]|[[Jennifer Holcom...|[shawnee national...| [[Shawnee Nationa...|[4.8672566, 5.309...|[[4, 3, 25, 0, 11...|[[wc, 198.0], [c1...|http://d32dsh9a6d...|                  []|               []|[https://youtube....|             [[,,,]]|[[Campus Recreati...|[[100.0, dollars ...|            [,]|     null|nEaGRxRAJFaYjf/Cb...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|business-standard...|http://www.busine...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[ECON_OILPRICE, T...|[[GENERAL_GOVERNM...|[[COUNTRY, Iraq, ...|[[[COUNTRY, Libya...|[jack stubbs, dav...|[[Jack Stubbs, 20...|[american petrole...| [[American Petrol...|[-4.466501, 0.496...|            [[,,,,]]|[[wc, 360.0], [c1...|http://bsmedia.bu...|                  []|               []|[https://youtube....|             [[,,,]]|[[Chinese Lunar N...|[[60.0, dollars, ...|            [,]|     null|vAioMgkDAsxYbvJC8...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|       wordpress.com|https://lankapage...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[CYBER_ATTACK, TA...|[[GENERAL_GOVERNM...|[[COUNTRY, Qatar,...|[[[COUNTRY, Russi...|  [dmitry bestuzhev]|[[Dmitry Bestuzhe...|[global research,...| [[Global Research...|[-4.0816326, 1.42...|[[1, 0, 0, 2011, ...|[[wc, 427.0], [c1...|https://lankapage...|                  []|               []|                  []|             [[,,,]]|[[Middle East, 52...|[[50.0, countries...|            [,]|     null|gWwfu5Am3Ag/eQkBu...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|           emory.edu|http://news.emory...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_FNCACT, TAX_...|[[GENERAL_HEALTH,...|[[USSTATE, Massac...|[[[USSTATE, Texas...|[marian willinger...|[[Marian Willinge...|[national institu...| [[National Instit...|[-3.900156, 1.872...|[[1, 0, 0, 2003, ...|[[wc, 579.0], [c1...|http://news.emory...|                  []|               []|[https://youtube....|             [[,,,]]|[[National Instit...|[[6.0, months mos...|            [,]|     null|DSttGKEJHGoJq4JGU...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB| portlandmercury.com|http://www.portla...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_FNCACT, TAX_...|[[TAX_FNCACT, 205...|[[COUNTRY, United...|[[[USCITY, Manhat...|[eileen myles, ly...|[[Eileen Myles, 1...|[pacific northwes...| [[Pacific Northwe...|[0.0, 3.9772727, ...|[[4, 2, 21, 0, 23...|[[wc, 473.0], [c1...|http://www.portla...|                  []|               []|                  []|             [[,,,]]|[[Lower Manhattan...|[[200.0, dollars,...|            [,]|     null|nUqRcuewKF/t0jSoG...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|     timeslive.co.za|http://www.timesl...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[CRIME_ILLEGAL_DR...|[[CRIME_ILLEGAL_D...|      [[,,,,, [,],]]|   [[[,,,,, [,],],]]|     [tamas horvath]|[[Tamas Horvath, ...|   [yale university]| [[Yale University...|[0.0, 3.1390135, ...|            [[,,,,]]|[[wc, 202.0], [c1...|http://www.timesl...|                  []|               []|                  []|[[26,, you're ful...|[[Tamas Horvath, ...|              [[,,]]|            [,]|     null|Mey7cIjMP9koOY9lv...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|        thetandd.com|http://thetandd.c...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[LEADER, TAX_FNCA...|[[TERROR, 226], [...|[[USCITY, Washing...|[[[USCITY, White ...|      [barack obama]|[[Barack Obama, 70]]|                  []|                [[,]]|[-9.734513, 0.884...|            [[,,,,]]|[[wc, 102.0], [c1...|http://bloximages...|                  []|               []|                  []|             [[,,,]]|[[President Barac...|              [[,,]]|            [,]|     null|rrbGGGkxq4RONxUA4...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|  washingtonpost.com|http://www.washin...|[[WOUND, 4, soldi...|[[[WOUND, 4, sold...|[MILITARY, TAX_WE...|[[TAX_WEAPONS, 41...|[[COUNTRY, United...|[[[COUNTRY, Afgha...|[sergeant bryan c...|[[Joshua Hargis, ...|[army rangers, ka...| [[Army Rangers, 4...|[-4.2763157, 3.28...|[[1, 0, 0, 2013, ...|[[wc, 825.0], [c1...|http://img.washin...|                  []|               []|                  []|[[195,, His perso...|[[Army Rangers, 5...|[[10.0, improvise...|            [,]|     null|yZq7MxnoipOrDMRB2...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|    globegazette.com|http://globegazet...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_FNCACT, TAX_...|[[TAX_FNCACT, 911...|      [[,,,,, [,],]]|   [[[,,,,, [,],],]]|[kevin mowbray, c...|[[Kevin Mowbray, ...|[berkshire hathaw...| [[Berkshire Hatha...|[0.99601597, 2.78...|[[1, 0, 0, 2014, ...|[[wc, 429.0], [c1...|http://bloximages...|                  []|               []|                  []|[[35,, is off to ...|[[Lee Enterprises...|[[70.0, sharehold...|            [,]|     null|TBBG59O2BUv8lmyRi...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|   dailytimes.com.pk|http://www.dailyt...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_FNCACT, TAX_...|[[GENERAL_HEALTH,...|[[WORLDCITY, Mang...|[[[WORLDCITY, Kar...|[zafar ejaz, abba...|[[Zafar Ejaz, 737...|[superintendent u...| [[Superintendent ...|[0.9756098, 4.390...|            [[,,,,]]|[[wc, 197.0], [c1...|http://www.dailyt...|                  []|               []|                  []|             [[,,,]]|[[Sindh Minister,...|              [[,,]]|            [,]|     null|ZFZ48EUjqE38jk4op...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|            waff.com|http://www.WAFF.c...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[NATURAL_DISASTER...|[[TAX_FNCACT, 596...|[[USSTATE, Florid...|[[[USSTATE, Flori...|[david bowie, rob...|[[David Bowie, 61...|[associated press...| [[Associated Pres...|[-3.3613446, 0.84...|            [[,,,,]]|[[wc, 109.0], [c1...|http://RAYCOMGROU...|                  []|               []|                  []|             [[,,,]]|[[South Florida, ...|              [[,,]]|            [,]|     null|kDiOly1M7H+YkDZyy...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|            kwtx.com|http://www.kwtx.c...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_WORLDLANGUAG...|[[TAX_WORLDLANGUA...|[[USCITY, Washing...|[[[COUNTRY, Mexic...|                  []|               [[,]]|                  []|                [[,]]|[-1.4184397, 0.0,...|            [[,,,,]]|[[wc, 128.0], [c1...|http://media.gray...|                  []|               []|                  []|             [[,,,]]|[[Colorado River,...|[[1000000.0, of p...|            [,]|     null|Zk8R0KnoOo+W8APvK...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|      al-monitor.com|http://www.al-mon...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_RELIGION, TA...|[[TERROR, 4252], ...|[[WORLDCITY, Gezi...|[[[COUNTRY, Turks...|[ayse bohurler, e...|[[Ayse Bohurler, ...|[european union, ...| [[European Union,...|[-4.4309297, 4.25...|[[1, 0, 0, 1931, ...|[[wc, 1067.0], [c...|http://www.al-mon...|                  []|               []|                  []|[[191,, This prop...|[[Old Turkey, 383...|[[2.0, key argume...|            [,]|     null|c0WJjIzf5zjruRPEy...|  null|
|[2015-02-18 23:00...|2015-02-18 23:00:00|                       WEB|          sltrib.com|http://www.sltrib...|[[,,, [,,,,, [,],]]]|[[[,,, [,,,,, [,]...|[TAX_FNCACT, TAX_...|[[BAN, 236], [TAX...|      [[,,,,, [,],]]|   [[[,,,,, [,],],]]|                  []|               [[,]]|                  []|                [[,]]|[-10.204082, 0.0,...|            [[,,,,]]|[[wc, 86.0], [c12...|http://www.sltrib...|                  []|               []|[https://youtube....|[[29,, Flag comme...|   [[Salt Lake, 87]]|              [[,,]]|            [,]|     null|kIAVlR9o5nny9axib...|  null|
+--------------------+-------------------+--------------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+---------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-----------------+--------------------+--------------------+--------------------+--------------------+---------------+---------+--------------------+------+

```
Example of `Dataset[GKGCount]`
```
+-------------------+-----------+--------------------+--------------------+--------------------+--------------------+
|        publishDate|numArticles|              counts|            eventIds|             sources|          sourceUrls|
+-------------------+-----------+--------------------+--------------------+--------------------+--------------------+
|2020-07-16 00:00:00|          1|[CRISISLEX_T03_DE...|         [935532003]|     [xinhuanet.com]|[http://www.xinhu...|
|2020-07-16 00:00:00|          1|[WOUND, 5,, [COUN...|                  []|     [express.co.uk]|[https://www.expr...|
|2020-07-16 00:00:00|          1|[KILL, 154,, [USC...|                  []|            [nj.com]|[https://www.nj.c...|
|2020-07-16 00:00:00|          1|[WOUND, 199,, [US...|                  []|      [fox13now.com]|[https://www.fox1...|
|2020-07-16 00:00:00|          3|[AFFECT, 11, reve...|[935346960, 93534...|[baltimoresun.com...|[http://www.balti...|
|2020-07-16 00:00:00|          2|[CRISISLEX_CRISIS...|                  []| [msn.com, wibw.com]|[https://www.msn....|
|2020-07-16 00:00:00|          1|[CRISISLEX_CRISIS...|[935479127, 93553...|         [omaha.com]|[https://omaha.co...|
|2020-07-16 00:00:00|          1|[WOUND, 774,, [US...|[935488826, 93535...|        [wrcbtv.com]|[https://www.wrcb...|
|2020-07-16 00:00:00|          3|[KILL, 777, moves...|                  []|[telegraphherald....|[https://www.tele...|
|2020-07-16 00:00:00|          4|[CRISISLEX_T03_DE...|[935457320, 93545...|[prokerala.com, p...|[https://www.prok...|
|2020-07-16 00:00:00|          1|[CRISISLEX_T03_DE...|         [935532003]|     [xinhuanet.com]|[http://www.xinhu...|
|2020-07-16 00:00:00|          1|[WOUND, 5,, [COUN...|                  []|     [express.co.uk]|[https://www.expr...|
|2020-07-16 00:00:00|          1|[KILL, 154,, [USC...|                  []|            [nj.com]|[https://www.nj.c...|
|2020-07-16 00:00:00|          1|[WOUND, 199,, [US...|                  []|      [fox13now.com]|[https://www.fox1...|
|2020-07-16 00:00:00|          3|[AFFECT, 11, reve...|[935346960, 93534...|[baltimoresun.com...|[http://www.balti...|
|2020-07-16 00:00:00|          2|[CRISISLEX_CRISIS...|                  []| [msn.com, wibw.com]|[https://www.msn....|
|2020-07-16 00:00:00|          1|[CRISISLEX_CRISIS...|[935479127, 93553...|         [omaha.com]|[https://omaha.co...|
|2020-07-16 00:00:00|          1|[WOUND, 774,, [US...|[935488826, 93535...|        [wrcbtv.com]|[https://www.wrcb...|
|2020-07-16 00:00:00|          3|[KILL, 777, moves...|                  []|[telegraphherald....|[https://www.tele...|
|2020-07-16 00:00:00|          4|[CRISISLEX_T03_DE...|[935457320, 93545...|[prokerala.com, p...|[https://www.prok...|
+-------------------+-----------+--------------------+--------------------+--------------------+--------------------+
```
Example of `Dataset[MentionV2]`
```
+---------+-------------------+-------------------+-----------+--------------------+--------------------+----------+----------------+----------------+----------------+---------+----------+-------------+--------------+--------------------+------+
|  eventId|          eventTime|        mentionTime|mentionType|   mentionSourceName|   mentionIdentifier|sentenceId|actor1CharOffset|actor2CharOffset|actionCharOffset|inRawText|confidence|mentionDocLen|mentionDocTone|                hash|errors|
+---------+-------------------+-------------------+-----------+--------------------+--------------------+----------+----------------+----------------+----------------+---------+----------+-------------+--------------+--------------------+------+
|410412347|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB| dailymaverick.co.za|http://www.dailym...|        19|              -1|            4594|            4634|        1|        50|         6665|     -4.477612|vq06mMG2SPajdEwgF...|  null|
|410412348|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|      indiatimes.com|http://timesofind...|         2|              -1|             300|             344|        1|        50|         2541|      2.078522|ru9zgEy3ZMArwWzMi...|  null|
|410412349|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|          voxy.co.nz|http://www.voxy.c...|         4|              -1|            1297|            1232|        0|        10|         2576|      7.517084|Ng58/bGSFM5UKXcf0...|  null|
|410412350|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|          voxy.co.nz|http://www.voxy.c...|         4|              -1|            1298|            1233|        1|        20|         2576|      7.517084|/0iZMoDAfPFyTl5rB...|  null|
|410412351|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|   eastidahonews.com|http://www.eastid...|         1|              -1|             103|             122|        1|       100|         1432|           0.0|4b7D43yYqS6nj43Jg...|  null|
|410368256|2015-02-18 19:30:00|2015-02-18 23:00:00|        WEB|businessinsider.c...|http://www.busine...|         4|            1325|              -1|            1356|        0|        20|         1628|           0.0|8wNyMOYFsDz+QQj31...|  null|
|410290898|2015-02-18 14:15:00|2015-02-18 23:00:00|        WEB|blayneychronicle....|http://www.blayne...|        12|            2675|              -1|            2705|        1|        50|         3345|     0.5300353|02TeaefqfDjM2dtyM...|  null|
|410290898|2015-02-18 14:15:00|2015-02-18 23:00:00|        WEB|murrayvalleystand...|http://www.murray...|        12|            2675|              -1|            2705|        1|        50|         3345|     0.5300353|bLE1wSlzKZmwW9mb0...|  null|
|410290898|2015-02-18 14:15:00|2015-02-18 23:00:00|        WEB| stawelltimes.com.au|http://www.stawel...|        12|            2675|              -1|            2705|        1|        50|         3345|     0.5300353|29L5KqVVbZ05f9WSX...|  null|
|410290898|2015-02-18 14:15:00|2015-02-18 23:00:00|        WEB|goondiwindiargus....|http://www.goondi...|        12|            2675|              -1|            2705|        1|        50|         3345|     0.5300353|hE6QLN4Yea5yl9NjP...|  null|
|410290898|2015-02-18 14:15:00|2015-02-18 23:00:00|        WEB|cessnockadvertise...|http://www.cessno...|        12|            2675|              -1|            2705|        1|        50|         3345|     0.5300353|fYSjxxqjBP3vXpAiH...|  null|
|410290898|2015-02-18 14:15:00|2015-02-18 23:00:00|        WEB|whyallanewsonline...|http://www.whyall...|        12|            2675|              -1|            2705|        1|        50|         3345|     0.5300353|H1+4eCxSzY4nPo8oY...|  null|
|410290898|2015-02-18 14:15:00|2015-02-18 23:00:00|        WEB|victorharbortimes...|http://www.victor...|        12|            2675|              -1|            2705|        1|        50|         3345|     0.5300353|93/fFgyI/4PO8P25s...|  null|
|410412352|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|businessspectator...|http://www.busine...|         4|            1782|            1807|            1802|        0|        20|         3850|     -1.263823|2vespO2w4oRUhISuz...|  null|
|410405468|2015-02-18 22:30:00|2015-02-18 23:00:00|        WEB|businessinsider.c...|http://www.busine...|         4|            1303|            1273|            1337|        1|        50|         1628|           0.0|HfcrVd4uTRdTW/0ox...|  null|
|410412353|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|          voxy.co.nz|http://www.voxy.c...|         4|            1228|            1195|            1218|        0|        10|         2576|      7.517084|YzqUcug1U91bO0ET9...|  null|
|410412354|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|          voxy.co.nz|http://www.voxy.c...|         4|            1250|            1329|            1267|        0|        10|         2576|      7.517084|irGA4l4V1nsgmOtU5...|  null|
|410412355|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|          voxy.co.nz|http://www.voxy.c...|         4|            1235|            1314|            1252|        0|        10|         2576|      7.517084|eAy3/r3w0sbubcuNz...|  null|
|410412356|2015-02-18 23:00:00|2015-02-18 23:00:00|        WEB|          voxy.co.nz|http://www.voxy.c...|         4|            1213|            1180|            1203|        0|        10|         2576|      7.517084|5z2Ep4pq/kHFFHhI0...|  null|
|410358405|2015-02-18 18:45:00|2015-02-18 23:00:00|        WEB|    heraldsun.com.au|http://www.herald...|         5|            1779|            1834|            1789|        0|        10|         3048|    -2.2222223|7QcISt4vwRvw0z1N6...|  null|
+---------+-------------------+-------------------+-----------+--------------------+--------------------+----------+----------------+----------------+----------------+---------+----------+-------------+--------------+--------------------+------+

```

Example of `Dataset[EventNormDaily]`
```
+----------+----------+
|       day|eventCount|
+----------+----------+
|1920-01-01|     87765|
|1920-01-02|    121054|
|1920-01-03|    153580|
|1920-01-04|    110228|
|1920-01-05|     63362|
|1920-01-06|        85|
|1979-01-01|       661|
|1979-01-02|       976|
|1979-01-03|      1060|
|1979-01-04|       950|
|1979-01-05|      1027|
|1979-01-06|       644|
|1979-01-07|       884|
|1979-01-08|      1291|
|1979-01-09|      1287|
|1979-01-10|      1290|
|1979-01-11|       866|
|1979-01-12|      1443|
|1979-01-13|       726|
|1979-01-14|       851|
+----------+----------+
```

Example of `Dataset[EventNormDailyByCountry]`
```
+----------+-----------+----------+
|       day|countryCode|eventCount|
+----------+-----------+----------+
|1920-01-01|       null|      2396|
|1920-01-01|         AC|        10|
|1920-01-01|         AE|       200|
|1920-01-01|         AF|       699|
|1920-01-01|         AG|        55|
|1920-01-01|         AJ|        48|
|1920-01-01|         AL|        20|
|1920-01-01|         AM|        37|
|1920-01-01|         AO|        76|
|1920-01-01|         AR|        68|
|1920-01-01|         AS|      2650|
|1920-01-01|         AU|       165|
|1920-01-01|         AY|        12|
|1920-01-01|         BA|        72|
|1920-01-01|         BB|        47|
|1920-01-01|         BC|        18|
|1920-01-01|         BD|         9|
|1920-01-01|         BE|       104|
|1920-01-01|         BF|        68|
|1920-01-01|         BG|       456|
+----------+-----------+----------+

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

### Accessing HTML content

The main difference between an average and an expert data scientist is the level of curiosity and creativity employed in extracting the value latent in the data. 
You could build a simple model on top of GDELT, or you could notice and leverage all these URLs mentioned, scrape that content, and use these extended results to discover new insights that exceed the original questions.

We delegate this logic to the excellent Scala library [Goose](https://github.com/GravityLabs/goose/wiki) 

I decided to embed my own `Goose` library in that project for the following reasons

1. to make it `Scala 2.11` compatible (currently `2.9`)
2. to start tuning it towards the extraction of news content and not generic HTML
3. to start cleaning its messy code base (sorry guys, love this library but code is a mess!)
4. to scale it for spark (distributed news scanner)

##### The Goose library

Interacting with the Goose library is fairly easy

```scala
import com.gravity.goose.{Configuration, Goose}

val conf: Configuration = new Configuration
conf.setEnableImageFetching(false)
conf.setBrowserUserAgent("Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8")
conf.setConnectionTimeout(1000)
conf.setSocketTimeout(1000)

val url = "http://www.bbc.co.uk/news/entertainment-arts-35278872"
val goose: Goose = new Goose(conf)
val article = goose.extractContent(url)
```

##### Spark Pipeline

Scraping news articles from URLs in above datasets is done via a Spark [ML pipeline](https://spark.apache.org/docs/2.2.0/ml-pipeline.html)

```scala
import com.aamend.spark.gdelt.ContentFetcher

val contentFetcher = new ContentFetcher()
  .setInputCol("sourceUrl")
  .setOutputTitleCol("title")
  .setOutputContentCol("content")
  .setOutputKeywordsCol("keywords")
  .setOutputPublishDateCol("publishDate")
  .setOutputDescriptionCol("description")
  .setUserAgent("Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8")
  .setConnectionTimeout(1000)
  .setSocketTimeout(1000)

val contentDF = contentFetcher.transform(gdeltEventDS)
contentDF.show()
```

The resulting dataframe is as follows

```
+--------------------+--------------------+--------------------+--------------------+--------------------+------------+
|           sourceUrl|         description|             content|            keywords|               title| publishDate|
+--------------------+--------------------+--------------------+--------------------+--------------------+------------+
|https://www.thegu...|Parliament passes...|Mariano Rajoy, on...|[MARIANO RAJOY, S...|Mariano Rajoy ous...|  2018-06-01|
+--------------------+--------------------+--------------------+--------------------+--------------------+------------+
```

##### Fetching Images

With a proper installation of [imagemagick](http://www.imagemagick.org/script/index.php), this library can even detect the most representative picture of a given article. 
Downloading GDELT image header for each article opens up lots of data science opportunities (face recognition, fake news / propaganda detection).
This, however, requires an installation of `imagemagick` on all executors across your Spark cluster.

```scala
import com.aamend.spark.gdelt.ContentFetcher

val contentFetcher = new ContentFetcher()
  .setInputCol("sourceUrl")
  .setOutputImageUrlCol("imageUrl")
  .setOutputImageBase64Col("imageBase64")
  .setImagemagickConvert("/usr/local/bin/convert")  
  .setImagemagickIdentify("/usr/local/bin/identify")
  
val contentDF = contentFetcher.transform(gdeltEventDS)
println(contentDF.rdd.first.getAs[String]("imageBase64"))
```

The main image is represented as base64 in data URI

```
data:image/jpeg;width=300;height=180;base64,/9j/4AAQSkZJRgABAQEASABIAA...
```

You can validate it via any online viewer such as [https://codebeautify.org/base64-to-image-converter](https://codebeautify.org/base64-to-image-converter)

![article](/images/article.jpeg)

*source: https://www.theguardian.com/world/2018/jun/01/mariano-rajoy-ousted-as-spain-prime-minister*

##### Parameters

+ `setInputCol(s: String)`

Mandatory, the input column containing URLs to fetch

+ `setOutputTitleCol(s: String)`

Optional, the output column to store article title - title will not be fetched if not specified

+ `setOutputContentCol(s: String)`

Optional, the output column to store article content - content will not be fetched if not specified

+ `setOutputKeywordsCol(s: String)`

Optional, the output column to store article metadata keyword - keywords will not be fetched if not specified

+ `setOutputPublishDateCol(s: String)` 

Optional, the output column to store article metadata publishDate - date will not be fetched if not specified

+ `setOutputDescriptionCol(s: String)`

Optional, the output column to store article metadata description - description will not be fetched if not specified

+ `setOutputImageUrlCol(s: String)`

Optional, the output column to store article main image URL - image fetching will be disabled if not specified

+ `setOutputImageBase64Col(s: String)`

Optional, the output column to store article main image data URI - image fetching will be disabled if not specified

+ `setImagemagickConvert(s: String)`

Optional, the path to imagemagick `convert` executable on every spark executors, not used if image fetching is disabled, default: `/usr/local/bin/convert`

+ `setImagemagickIdentify(s: String)`

Optional, the path to imagemagick `identify` executable on every spark executors, not used if image fetching is disabled, default: `/usr/local/bin/identify`

+ `setUserAgent(s: String)` 

Optional, the user agent to use in Goose HTTP requests, default: `Mozilla/5.0`

+ `setConnectionTimeout(i: Int)`

Optional, the connection timeout, in milliseconds, default: `1000`

+ `setSocketTimeout(i: Int)`

Optional, the socket timeout, in milliseconds, default: `1000`

#### Fueling data science use cases

The main reason I decided to embed a Goose extraction as a ML pipeline is to integrate news content with existing Spark ML functionality. 
In below example, we extract content of web articles, tokenize words and train a [Word2Vec](https://spark.apache.org/docs/2.2.0/ml-features.html#word2vec) model

```scala
val contentFetcher = new ContentFetcher()
  .setInputCol("sourceUrl")
  .setOutputContentCol("content")

val tokenizer = new Tokenizer()
  .setInputCol("content")
  .setOutputCol("words")

val word2Vec = new Word2Vec()
  .setInputCol("words")
  .setOutputCol("features")

val pipeline = new Pipeline()
  .setStages(Array(contentFetcher, tokenizer, word2Vec))
  
val model = pipeline.fit(df)
val words = model.transform(df)
```

## Version

+ 1.0 - Basic parser and reference data
+ 2.0 - Adding Goose library packaged for Spark / Scala 2.11

## Authors

Antoine Amend - [[antoine.amend@gmail.com]](antoine.amend@gmail.com)

## License

Apache License, version 2.0
