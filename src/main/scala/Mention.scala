package fr.telecom

import fr.telecom.DataMangling._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

// GDELT Mention schema extracted from BigQuery repo:
// bq show --format=prettyjson gdelt-bq:gdeltv2.eventmentions |
// jq '.["schema"]' |jq '.[]|.[]|[.name +": " + .type + ","]|.[]' |
// sed "s/INTEGER/Int/" | sed "s/FLOAT/Double/" | sed "s/STRING/String/" |sed "s/\"//g"
// > eventsmentions.txt
case class Mention(
                    GLOBALEVENTID: Int,
                    EventTimeDate: Long,

                    MentionTimeDate: Long,
                    MentionType: Int,
                    MentionSourceName: String,
                    MentionIdentifier: String,
                    SentenceID: Int,
                    // Actors
                    Actor1CharOffset: Int,
                    Actor2CharOffset: Int,
                    ActionCharOffset: Int,

                    InRawText: Int,
                    Confidence: Int,

                    MentionDocLen: Int,
                    MentionDocTone: Double,
                    MentionDocTranslationInfo: String,
                    // Future use
                    Extras: String,
                    // Added
                    SRCLC: String, // Subfield of MentionDocTranslationInfo
                    ENG: String // Subfield of MentionDocTranslationInfo
                  )

object Mention {
  def rddToDs(spark: SparkSession, mentionsRdd: RDD[String]): Dataset[Mention] = {

    import spark.implicits._

    mentionsRdd.map(_.split("\t")).filter(_.length >= 14).map(
      e => {
        val MentionDocTranslationInfo = if (e.length >= 15) e(14) else ""
        var SRCLC: String = ""
        var ENG: String = ""
        if(MentionDocTranslationInfo.isEmpty == false) {
          val split1 = MentionDocTranslationInfo.split(";")
          SRCLC = split1(0).substring(split1(0).indexOf(":") + 1)
          ENG = "" // ToDO
        }
        Mention(
          toInt(e(0)), toLong(e(1)), // Event
          toLong(e(2)), toInt(e(3)), e(4), e(5), toInt(e(6)),
          toInt(e(7)), toInt(e(8)), toInt(e(9)), // Actors
          toInt(e(10)), toInt(e(11)), // InRawText, Conf
          toInt(e(12)), toDouble(e(13)), MentionDocTranslationInfo, // MentionDoc
          "", // Extras
          SRCLC, ENG
        )
      }
    ).toDS
  }
}
