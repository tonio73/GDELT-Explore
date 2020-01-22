package fr.telecom

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object MainQueryC extends App {

  // Select files corresponding to reference period (as set in Context.scala)
  val spark = Context.createSession()
  val sc = spark.sparkContext

  val path = "20150222091500.gkg.csv.zip"

  val gkgRDD = Downloader.zipsToRdd(spark, path, "/home/mde/Documents/Projet_Furet/")
  val gkgDs = GKG.rddToDs(spark, gkgRDD)


  val themeDs: DataFrame = gkgDs
    //.drop(gkgDs.col("SourceCollectionIdentifier"))
    .drop(gkgDs.col("DocumentIdentifier"))
    .drop(gkgDs.col("Counts"))
    .drop(gkgDs.col("V2Counts"))
    .drop(gkgDs.col("Locations"))
    .drop(gkgDs.col("V2Locations"))
    .drop(gkgDs.col("Persons"))
    .drop(gkgDs.col("V2Persons"))
    .drop(gkgDs.col("Organizations"))
    .drop(gkgDs.col("V2Organizations"))
    .drop(gkgDs.col("Dates"))
    .drop(gkgDs.col("GCAM"))
    .drop(gkgDs.col("SharingImage"))
    .drop(gkgDs.col("RelatedImages"))
    .drop(gkgDs.col("SocialImageEmbeds"))
    .drop(gkgDs.col("SocialVideoEmbeds"))
    .drop(gkgDs.col("Quotations"))
    .drop(gkgDs.col("AllNames"))
    .drop(gkgDs.col("Amounts"))
    .drop(gkgDs.col("TranslationInfo"))
    .drop(gkgDs.col("Extras"))


  val newDs2 = themeDs
    .withColumn("Themes2", split(col("Themes"), ";"))
    .withColumn("themesDef", explode(col("Themes2")))
    .withColumn("temp", split(col("V2Tone"), ","))
    .withColumn("V2ToneMean", col("temp")(0).cast("Float"))
    .na.drop()

  //REQ C BY THEME
  val reqCtheme = newDs2
    .groupBy("DATE","SourceCommonName","themesDef")
    .agg(count("GKGRECORDID").alias("nbArticle"),mean("V2ToneMean").alias("toneMean"))


  reqCtheme.write.mode("overwrite").csv(Context.outputPath + "/reqCtheme_csv")

  val personDs: DataFrame = gkgDs
    .drop(gkgDs.col("SourceCollectionIdentifier"))
    .drop(gkgDs.col("DocumentIdentifier"))
    .drop(gkgDs.col("Counts"))
    .drop(gkgDs.col("V2Counts"))
    .drop(gkgDs.col("Locations"))
    .drop(gkgDs.col("V2Locations"))
    .drop(gkgDs.col("Themes"))
    .drop(gkgDs.col("V2Themes"))
    .drop(gkgDs.col("Organizations"))
    .drop(gkgDs.col("V2Organizations"))
    .drop(gkgDs.col("Dates"))
    .drop(gkgDs.col("GCAM"))
    .drop(gkgDs.col("SharingImage"))
    .drop(gkgDs.col("RelatedImages"))
    .drop(gkgDs.col("SocialImageEmbeds"))
    .drop(gkgDs.col("SocialVideoEmbeds"))
    .drop(gkgDs.col("Quotations"))
    .drop(gkgDs.col("AllNames"))
    .drop(gkgDs.col("Amounts"))
    .drop(gkgDs.col("TranslationInfo"))
    .drop(gkgDs.col("Extras"))

  val personDs2 = personDs
    .withColumn("persons2", split(col("Persons"), ";"))
    .withColumn("personsDef", explode(col("persons2")))
    .withColumn("temp", split(col("V2Tone"), ","))
    .withColumn("V2ToneMean", col("temp")(0).cast("Float"))
    .na.drop()

  //REQUETE C BY PERSON
  val reqCperson = personDs2
    .groupBy("DATE", "SourceCommonName", "personsDef")
    .agg(count("GKGRECORDID").alias("nbArtcile"),mean("V2ToneMean").alias("toneMean"))


  reqCperson.write.mode("overwrite").csv(Context.outputPath + "/reqCperson_csv")

  val countryDs: DataFrame = gkgDs
    .drop(gkgDs.col("SourceCollectionIdentifier"))
    .drop(gkgDs.col("DocumentIdentifier"))
    .drop(gkgDs.col("Counts"))
    .drop(gkgDs.col("V2Counts"))
    .drop(gkgDs.col("Persons"))
    .drop(gkgDs.col("V2Persons"))
    .drop(gkgDs.col("Themes"))
    .drop(gkgDs.col("V2Themes"))
    .drop(gkgDs.col("Organizations"))
    .drop(gkgDs.col("V2Organizations"))
    .drop(gkgDs.col("Dates"))
    .drop(gkgDs.col("GCAM"))
    .drop(gkgDs.col("SharingImage"))
    .drop(gkgDs.col("RelatedImages"))
    .drop(gkgDs.col("SocialImageEmbeds"))
    .drop(gkgDs.col("SocialVideoEmbeds"))
    .drop(gkgDs.col("Quotations"))
    .drop(gkgDs.col("AllNames"))
    .drop(gkgDs.col("Amounts"))
    .drop(gkgDs.col("TranslationInfo"))
    .drop(gkgDs.col("Extras"))

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


  val CombinationsCountriesUdf = udf(CombinationsCountries _)

  val countryDs2 = countryDs
    .withColumn("michel", CombinationsCountriesUdf(col("V2Locations")))
    .withColumn("country", explode(col("michel")))
    .withColumn("temp", split(col("V2Tone"), ","))
    .withColumn("V2ToneMean", col("temp")(0).cast("Float"))
    .na.drop()


  val reqCcountry = countryDs2
    .groupBy(col("DATE"),col("SourceCommonName"),col("country"))
    .agg(count("GKGRECORDID").alias("nbArticle"),mean("V2ToneMean").alias("toneMean"))


  reqCcountry.write.mode("overwrite").csv(Context.outputPath + "reqCcountry_csv")
                           
}