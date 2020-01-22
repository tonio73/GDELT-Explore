package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._

object MainQueryB extends App {

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
    val spark = Context.createSession(localMaster, cassandraIp)

    import spark.implicits._

    // Read GDELT compressed CSV (currently from /tmp/data, later from S3)
    val eventsRDD: RDD[String] = Downloader.zipsToRdd(spark, refPeriod + "[0-9]*.export.CSV.zip", fromS3)
    val mentionsRDD: RDD[String] = Downloader.zipsToRdd(spark, refPeriod + "[0-9]*.mentions.CSV.zip", fromS3)

    val eventsDs: Dataset[Event] = Event.rddToDs(spark, eventsRDD)
    val mentionsDs: Dataset[Mention] = Mention.rddToDs(spark, mentionsRDD)

    // Request b) pour un pays donné en paramètre,
    // affichez les évènements qui y ont eu place triées par le nombre de mentions (tri décroissant);
    // permettez une aggrégation par jour/mois/année
    logger.info("Launch request b)")

    // First data to put into cassandra, the country parameter must be given to cassandra and not spark (need to do filter in cassandra)
    val res = eventsDs.as("events").join(mentionsDs.as("mentions"), $"events.GLOBALEVENTID" === $"mentions.GLOBALEVENTID", joinType = "left")
      .groupBy($"events.GLOBALEVENTID").count()
      .join(eventsDs, Seq("GLOBALEVENTID"), joinType = "left")
      .orderBy(col("count").desc)

    logger.info("Completed write of request b)")

    val columnNames = Seq("GLOBALEVENTID", "SQLDATE", "count")
    val cassandraCols = SomeColumns("globaleventid", "sqldate", "count")
    Uploader.persistDataFrame(fromS3, cassandraIp,
      res, columnNames,
      "reqB_csv",
      "test", "queryb", cassandraCols
    )

    logger.info("Finish request b)")
  }
  catch {
    // The call was transmitted successfully, but AWS couldn't process
    // it, so it returned an error response.
    case e: AmazonServiceException => e.printStackTrace();
    // Amazon S3 couldn't be contacted for a response, or the client
    // couldn't parse the response from AWS.
    case e: SdkClientException => e.printStackTrace();
  }

  logger.info("Fu-fu program completed")
}