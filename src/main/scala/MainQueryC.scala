package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}
import com.datastax.spark.connector._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object MainQueryC extends App {

  val logger = Context.logger

  try {

    var localMaster = false
    var fromS3 = false
    var cassandraIp = ""
    var refPeriod = Context.refPeriod()
    var i: Int = 0
    while (i < args.length) {
      args(i) match {
        case "--local-master" => localMaster = true

        case "--from-s3" => fromS3 = true

        case "--cassandra-ip" => {
          i += 1
          cassandraIp = args(i)
        }

        case "--ref-period" => {
          i += 1
          refPeriod = args(i)
        }

        case _ => {
          print("Unknown argument " + args(i) + "\n")
          print("Usage: --index to download master files\n")
        }
      }
      i += 1
    }

    logger.info("Create Spark session")

    // Select files corresponding to reference period (as set in Context.scala)
    val spark = Context.createSession("GDELT-ETL-MainQueryC", localMaster, cassandraIp)

    val gkgRDD = Downloader.zipsToRdd(spark, refPeriod + "*.gkg.csv.zip", fromS3)

    val gkgDs = GKG.rddToDs(spark, gkgRDD)

    logger.info("Launch request c) theme")
    requestCByTheme(spark, gkgDs, fromS3, cassandraIp)

    logger.info("Launch request c) person")
    requestCByPerson(spark, gkgDs, fromS3, cassandraIp)

    logger.info("Launch request c) country")
    requestCByCountry(spark, gkgDs, fromS3, cassandraIp)

    logger.info("Fu-fu program completed")
  }
  catch {
    // The call was transmitted successfully, but AWS couldn't process
    // it, so it returned an error response.
    case e: AmazonServiceException => e.printStackTrace();
    // Amazon S3 couldn't be contacted for a response, or the client
    // couldn't parse the response from AWS.
    case e: SdkClientException => e.printStackTrace();
  }

  // REQUEST C BY THEME
  def requestCByTheme(spark: SparkSession, gkgDs: Dataset[GKG], fromS3: Boolean, cassandraIp: String) = {

    val themeDs: DataFrame = gkgDs
      .withColumn("DATE", substring(col("DATE"),1,8))
      .withColumn("year", substring(col("DATE"),1,4))
      .withColumn("month", substring(col("DATE"),5,6))
      .withColumn("day", substring(col("DATE"),7,8))
      //.drop(gkgDs.col("SourceCollectionIdentifier"))
      .drop("DocumentIdentifier", "Counts", "V2Counts", "Locations", "V2Locations", "Persons", "V2Persons",
        "Organizations", "V2Organizations", "Dates", "GCAM", "SharingImage", "RelatedImages", "SocialImageEmbeds",
        "SocialVideoEmbeds", "Quotations", "AllNames", "Amounts", "TranslationInfo", "Extras")

    val newDs2 = themeDs
      .withColumn("Themes2", split(col("Themes"), ";"))
      .withColumn("themesDef", explode(col("Themes2")))
      .withColumn("temp", split(col("V2Tone"), ","))
      .withColumn("V2ToneMean", col("temp")(0).cast("Float"))
      .na.drop()

    val reqCtheme = newDs2
      .groupBy("DATE","year",  "month","day", "SourceCommonName", "themesDef")
      .agg(count("GKGRECORDID").alias("nbArticle"), mean("V2ToneMean").alias("toneMean"))

    val columnNamesTheme = Seq("DATE", "year", "month","day","SourceCommonName", "themesDef", "nbArticle", "toneMean")

    val cassandraColumnsTheme = SomeColumns("date", "year", "month","day", "source", "theme", "nbarticle","tonemean")
    Uploader.persistDataFrame(fromS3, cassandraIp, reqCtheme, columnNamesTheme,
      "reqCtheme_csv",
      "gdelt", "queryctheme", cassandraColumnsTheme)
  }

  // REQUEST C BY PERSON
  def requestCByPerson(spark: SparkSession, gkgDs: Dataset[GKG], fromS3: Boolean, cassandraIp: String) = {

    val personDs: DataFrame = gkgDs
      .withColumn("DATE", substring(col("DATE"),1,8))
      .withColumn("year", substring(col("DATE"),1,4))
      .withColumn("month", substring(col("DATE"),5,6))
      .withColumn("day", substring(col("DATE"),7,8))
      .drop("SourceCollectionIdentifier", "DocumentIdentifier", "Counts", "V2Counts",
        "Locations", "V2Locations", "Themes", "V2Themes", "Organizations", "V2Organizations",
        "Dates", "GCAM", "SharingImage", "RelatedImages", "SocialImageEmbeds", "SocialVideoEmbeds", "Quotations",
        "AllNames", "Amounts", "TranslationInfo", "Extras")

    val personDs2 = personDs
      .withColumn("persons2", split(col("Persons"), ";"))
      .withColumn("personsDef", explode(col("persons2")))
      .withColumn("temp", split(col("V2Tone"), ","))
      .withColumn("V2ToneMean", col("temp")(0).cast("Float"))
      .na.drop()

    val reqCperson = personDs2
      .groupBy("DATE","year","month","day", "SourceCommonName", "personsDef")
      .agg(count("GKGRECORDID").alias("nbArticle"), mean("V2ToneMean").alias("toneMean"))


    val columnNamesPerson = Seq("DATE", "year", "month","day" ,"SourceCommonName", "personsDef", "nbArticle","toneMean")
    val cassandraColumnsPerson = SomeColumns("date", "year", "month","day", "source", "person", "nbarticle","tonemean")
    Uploader.persistDataFrame(fromS3, cassandraIp, reqCperson, columnNamesPerson,
      "reqCperson_csv",
      "gdelt", "querycperson", cassandraColumnsPerson)
  }

  // REQUEST C BY COUNTRY
  def requestCByCountry(spark: SparkSession, gkgDs: Dataset[GKG], fromS3: Boolean, cassandraIp: String) = {

    val countryDs: DataFrame = gkgDs
      .withColumn("DATE", substring(col("DATE"),1,8))
      .withColumn("year", substring(col("DATE"),1,4))
      .withColumn("month", substring(col("DATE"),5,6))
      .withColumn("day", substring(col("DATE"),7,8))
      .drop("SourceCollectionIdentifier", "DocumentIdentifier", "Counts", "V2Counts",
        "Persons", "V2Persons", "Themes", "V2Themes", "Organizations", "V2Organizations", "Dates", "GCAM",
        "SharingImage", "RelatedImages", "SocialImageEmbeds", "SocialVideoEmbeds", "Quotations", "AllNames", "Amounts",
        "TranslationInfo", "Extras")

    val CombinationsCountriesUdf = udf(CombinationsCountries _)

    val countryDs2 = countryDs
      .withColumn("michel", CombinationsCountriesUdf(col("V2Locations")))
      .withColumn("country", explode(col("michel")))
      .withColumn("temp", split(col("V2Tone"), ","))
      .withColumn("V2ToneMean", col("temp")(0).cast("Float"))
      .na.drop()

    val reqCcountry = countryDs2
      .groupBy("DATE","year",  "month","day", "SourceCommonName", "country")
      //.groupBy(col("DATE"), col("year"), col("month"), col("day"), col("SourceCommonName"), col("country"))
      .agg(count("GKGRECORDID").alias("nbArticle"), mean("V2ToneMean").alias("toneMean"))


    val columnNamesCountry = Seq("DATE","year","month","day", "SourceCommonName", "country", "nbArticle", "toneMean")
    val cassandraColumnsCountry = SomeColumns("date", "year", "month", "day", "source", "country", "nbarticle","tonemean")
    Uploader.persistDataFrame(fromS3, cassandraIp, reqCcountry, columnNamesCountry,
      "reqCcountry_csv",
      "gdelt", "queryccountry", cassandraColumnsCountry)
  }

  /**
   * Iteration sur les chaines de caractères de l'Array divisé (split) par ","
   * pour créer la liste des pays mensionnés dans l'article
   */
  def CombinationsCountries(string_countries: String): List[String] = {

    val array_countries: Array[String] = string_countries.split("[,;]")
    val countries = ArrayBuffer[String]()
    for (s1: String <- array_countries) {
      var country: String = ""
      if (s1.matches(".*#.*#.*")) {
        if (s1.split("#")(0).trim.forall(_.isDigit)) {
          country = s1.split("#")(1).trim
        }
        else {
          country = s1.split("#")(0).trim
        }
        if (countries.find(x => x == country) == None) {
          countries += country
        }
      }
    }
    countries.toList
  }
}