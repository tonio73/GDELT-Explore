package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object MainQueryD extends App {

  /**
   *
   * Iteration sur les chaines de caractères de l'Array divisé (split) par ","
   * pour créer la liste des pays mensionnés dans l'article
   */
  def CombinationsCountries(string_countries: String): List[List[String]] = {

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

    // Select files corresponding to reference period (as set in Context.scala)
    val spark = Context.createSession(localMaster, cassandraIp)

    import spark.implicits._

    val gkgRDD: RDD[String] = Downloader.zipsToRdd(spark, refPeriod + "*.gkg.csv.zip", fromS3)

    val gkgDs: Dataset[GKG] = GKG.rddToDs(spark, gkgRDD)

    val gkgDsProjection = gkgDs.select("GKGRECORDID", "DATE" , "V2Tone", "V2Locations")
      .withColumn("temp", split(col("V2Tone"), ","))
      .withColumn("V2ToneMean", col("temp")(0).cast("Double"))

    logger.info("Launch request d)")

    val CombinationsCountriesUDF = udf(CombinationsCountries _)

    val reqD = gkgDsProjection.withColumn("CountriesTmp", CombinationsCountriesUDF($"V2Locations"))
      .withColumn("CountriesTmp", CombinationsCountriesUDF($"V2Locations"))
      .withColumn("countries", explode($"CountriesTmp"))
      .groupBy("countries", "DATE").agg(mean("V2ToneMean").alias("Ton moyen"),
                                       count("V2ToneMean").alias("Nombre d'articles"))
      .withColumn("country1", col("countries")(0))
      .withColumn("country2", col("countries")(1))
      .drop("countries")

    // Write
    val columnNames = Seq("SQLDATE", "ActionGeo_CountryCode", "SRCLC", "count") // TODO WITH CORRECT COLS in DF
    val cassandraColumns = SomeColumns("sqldate", "country", "language", "count") // TODO WITH CORRECT COLS in Cassandra, lower case
    Uploader.persistDataFrame(fromS3, cassandraIp, reqD, columnNames,
      "reqD_csv",
      "gdelt", "queryd", cassandraColumns)

    logger.info("Completed write of request d)")
  }


  catch {
    // The call was transmitted successfully, but AWS couldn't process
    // it, so it returned an error response.
    case e: AmazonServiceException => e.printStackTrace();
    // AWS couldn't be contacted for a response, or the client
    // couldn't parse the response from AWS.
    case e: SdkClientException => e.printStackTrace();
  }

  logger.info("Program completed")
}
