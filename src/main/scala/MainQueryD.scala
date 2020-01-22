package fr.telecom

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object MainQueryD extends App {

  def CombinationsCountries(string_countries: String): List[List[String]] = {
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

    val countries_list = countries.toList.combinations(2).toList

    val sorted: Seq[List[String]] = countries_list.map(x => x.sortWith(_ < _))

    sorted.toList
  }

  // Select files corresponding to reference period (as set in Context.scala)
  val spark = Context.createSession()

  import spark.implicits._

  val gkgRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "[0-9]*.gkg.csv.zip")

  val gkgDs: Dataset[GKG] = GKG.rddToDs(spark, gkgRDD)

  val gkgDsProjection = gkgDs.select("GKGRECORDID", "V2Tone", "V2Locations")
                              .withColumn("temp", split(col("V2Tone"), ","))
                               .withColumn("V2ToneMean",col("temp")(0).cast("Double"))


  println("Launch request d)")
  //.println(gkgDs.select("V2Locations").show(20, truncate = false))

  val CombinationsCountriesUDF = udf(CombinationsCountries _)


  val reqD = gkgDsProjection.withColumn("CountriesTmp", CombinationsCountriesUDF($"V2Locations"))
                        .withColumn("CountriesTmp", CombinationsCountriesUDF($"V2Locations"))
                        .withColumn("countries", explode($"CountriesTmp"))
                        .groupBy("countries").agg(
                                mean("V2ToneMean").alias("Ton moyen"),
                                count("V2ToneMean").alias("Nombre d'articles"))

                        .withColumn("country1", col("countries")(0))
                        .withColumn("country2", col("countries")(1))
                        .drop("countries")


  reqD.write.mode("overwrite").csv(Context.outputPath + "/reqD_csv")

  println(reqD.show(20))

  println("Completed write of request d)")

  /*
  eventsDs.createOrReplaceTempView("export")
  //spark.catalog.cacheTable("export")

  spark.sql(
    """
    SELECT SQLDATE, Actor1Geo_CountryCode, GLOBALEVENTID
    FROM export ORDER BY SQLDATE DESC
    """).write.mode("overwrite").csv(Context.outputPath + "/reqTest1_csv")

  spark.sql("""
    SELECT SQLDATE, COUNT(*) as nbEvents
    FROM export_translation GROUP BY SQLDATE
    """).write.csv(Context.outputPath + "/reqTest2_csv")

  spark.sql("""
    SELECT SQLDATE, COUNT(*) as nbEvents
    FROM export GROUP BY SQLDATE
    """).write.mode("overwrite").csv(Context.outputPath + "/reqTest3_csv")
   */

  println("Program completed")
}
