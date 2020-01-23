package fr.telecom

import DataMangling._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

// GDELT Event data object extracted from BigQuery
case class GKG(
                GKGRECORDID : String,
                DATE : Long,
                SourceCollectionIdentifier : Int,
                SourceCommonName : String,
                DocumentIdentifier : String,
                Counts : String,
                V2Counts : String,
                Themes : String,
                V2Themes : String,
                Locations  : String,
                V2Locations : String,
                Persons  : String,
                V2Persons : String,
                Organizations  : String,
                V2Organizations  : String,
                V2Tone : String,
                Dates  : String,
                GCAM : String,
                SharingImage : String,
                RelatedImages :  String,
                SocialImageEmbeds :  String,
                SocialVideoEmbeds :  String,
                Quotations : String,
                AllNames : String,
                Amounts  : String,
                TranslationInfo  : String,
                Extras : String
              ) {
}

object GKG {

  def rddToDs(spark: SparkSession, eventsRDD: RDD[String]): Dataset[GKG] = {

    import spark.implicits._

    eventsRDD.map(_.split("\t")).filter(_.length >= 25).map(
      e => GKG(
        e(0), toLong(e(1)), toInt(e(2)), e(3), e(4),
        e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14),
        e(15), e(16), e(17), e(18), e(19), e(20), e(21), e(22), e(23), e(24),
        "","")
    ).toDS
  }
}