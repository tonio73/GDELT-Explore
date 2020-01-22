package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object MainQueryC extends App {

  val logger = Context.logger

  try {

    logger.info("Create Spark session")

    // Select files corresponding to reference period (as set in Context.scala)
    val spark = Context.createSession()

    val gkgRDD = Downloader.zipsToRdd(spark, Context.refPeriod() + "*.gkg.csv.zip")
    val gkgDs = GKG.rddToDs(spark, gkgRDD)

    logger.info("Launch request c) theme")
    requestCByTheme(spark, gkgDs)

    logger.info("Launch request c) person")
    requestCByPerson(spark, gkgDs)

    logger.info("Launch request c) country")
    requestCByCountry(spark, gkgDs)

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
  def requestCByTheme(spark: SparkSession, gkgDs: Dataset[GKG]) = {

    val themeDs: DataFrame = gkgDs
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
      .groupBy("DATE", "SourceCommonName", "themesDef")
      .agg(count("GKGRECORDID").alias("nbArticle"), mean("V2ToneMean").alias("toneMean"))

    reqCtheme.write.mode("overwrite").csv(Context.outputPath + "/reqCtheme_csv")
  }

  // REQUEST C BY PERSON
  def requestCByPerson(spark: SparkSession, gkgDs: Dataset[GKG]) = {
    val personDs: DataFrame = gkgDs
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
      .groupBy("DATE", "SourceCommonName", "personsDef")
      .agg(count("GKGRECORDID").alias("nbArtcile"), mean("V2ToneMean").alias("toneMean"))


    reqCperson.write.mode("overwrite").csv(Context.outputPath + "/reqCperson_csv")
  }

  // REQUEST C BY COUNTRY
  def requestCByCountry(spark: SparkSession, gkgDs: Dataset[GKG]) = {
    val countryDs: DataFrame = gkgDs
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
      .groupBy(col("DATE"), col("SourceCommonName"), col("country"))
      .agg(count("GKGRECORDID").alias("nbArticle"), mean("V2ToneMean").alias("toneMean"))

    reqCcountry.write.mode("overwrite").csv(Context.outputPath + "reqCcountry_csv")
  }

  // UDF to extract country from location
  def CombinationsCountries(string_countries: String): List[String] = {
    /**
     *
     * Iteration sur les chaines de caractères de l'Array divisé (split) par ","
     * pour créer la liste des pays mensionnés dans l'article
     */
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