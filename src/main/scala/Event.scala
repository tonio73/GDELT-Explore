package fr.telecom

import DataMangling._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

// GDELT Event data object extracted from BigQuery
case class Event(
                  GLOBALEVENTID: Int,
                  SQLDATE: Int,
                  MonthYear: Int,
                  Year: Int,
                  FractionDate: Double,
                  // Actor 1
                  Actor1Code: String,
                  Actor1Name: String,
                  Actor1CountryCode: String,
                  Actor1KnownGroupCode: String,
                  Actor1EthnicCode: String,
                  Actor1Religion1Code: String,
                  Actor1Religion2Code: String,
                  Actor1Type1Code: String,
                  Actor1Type2Code: String,
                  Actor1Type3Code: String,
                  // Actor 2
                  Actor2Code: String,
                  Actor2Name: String,
                  Actor2CountryCode: String,
                  Actor2KnownGroupCode: String,
                  Actor2EthnicCode: String,
                  Actor2Religion1Code: String,
                  Actor2Religion2Code: String,
                  Actor2Type1Code: String,
                  Actor2Type2Code: String,
                  Actor2Type3Code: String,
                  // EVENT ACTION ATTRIBUTES
                  IsRootEvent: Int,
                  EventCode: String,
                  EventBaseCode: String,
                  EventRootCode: String,
                  QuadClass: Int,
                  GoldsteinScale: Double,
                  NumMentions: Int,
                  NumSources: Int,
                  NumArticles: Int,
                  AvgTone: Double,
                  // EVENT GEOGRAPHY
                  Actor1Geo_Type: Int,
                  Actor1Geo_FullName: String,
                  Actor1Geo_CountryCode: String,
                  Actor1Geo_ADM1Code: String,
                  Actor1Geo_ADM2Code: String,
                  Actor1Geo_Lat: Double,
                  Actor1Geo_Long: Double,
                  Actor1Geo_FeatureID: String,
                  Actor2Geo_Type: Int,
                  Actor2Geo_FullName: String,
                  Actor2Geo_CountryCode: String,
                  Actor2Geo_ADM1Code: String,
                  Actor2Geo_ADM2Code: String,
                  Actor2Geo_Lat: Double,
                  Actor2Geo_Long: Double,
                  Actor2Geo_FeatureID: String,
                  ActionGeo_Type: Int,
                  ActionGeo_FullName: String,
                  ActionGeo_CountryCode: String,
                  ActionGeo_ADM1Code: String,
                  ActionGeo_ADM2Code: String,
                  ActionGeo_Lat: Double,
                  ActionGeo_Long: Double,
                  ActionGeo_FeatureID: String,
                  // DATA MANAGEMENT FIELDS
                  DATEADDED: BigInt,
                  SOURCEURL: String
                ) {
}

object Event {

  def rddToDs(spark: SparkSession, eventsRDD: RDD[String]): Dataset[Event] = {

    import spark.implicits._

    eventsRDD.map(_.split("\t")).filter(_.length == 61).map(
      e => Event(
        toInt(e(0)), toInt(e(1)), toInt(e(2)), toInt(e(3)), toDouble(e(4)),
        e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), // Actor 1
        e(15), e(16), e(17), e(18), e(19), e(20), e(21), e(22), e(23), e(24), // Actor 2
        toInt(e(25)), e(26), e(27), e(28), toInt(e(29)), toDouble(e(30)), toInt(e(31)),
        toInt(e(32)), toInt(e(33)), toDouble(e(34)), toInt(e(35)), e(36), e(37), e(38), e(39), toDouble(e(40)),
        toDouble(e(41)), e(42), toInt(e(43)), e(44), e(45), e(46), e(47), toDouble(e(48)), toDouble(e(49)), e(50),
        toInt(e(51)), e(52), e(53), e(54), e(55), toDouble(e(56)), toDouble(e(57)), e(58),
        toBigInt(e(59)), e(60)) // DATA MANAGEMENT FIELDS
    ).toDS
  }
}