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
    var i: Int = 0
    while (i < args.length) {
      args(i) match {
        case "--local-master" => localMaster = true

        case "--from-s3" => fromS3 = true

        case "--cassandra-ip" => {
          i += 1
          cassandraIp = args(i)
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
    val eventsRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "[0-9]*.export.CSV.zip", fromS3)
    val mentionsRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "[0-9]*.mentions.CSV.zip", fromS3)

    val eventsDs: Dataset[Event] = Event.rddToDs(spark, eventsRDD)
    val mentionsDs: Dataset[Mention] = Mention.rddToDs(spark, mentionsRDD)

    // println("For %s, number of events = %d, number of mentions = %d".format(Context.refPeriod(), eventsDs.count(), mentionsDs.count()))

    // Request a) afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).
    logger.info("Launch request b)")
    val country = "FR"

    // First data to put into cassandra, the country parameter must be given to cassandra and not spark (need to do filter in cassandra)
    val res = eventsDs.as("events").join(mentionsDs.as("mentions"), $"events.GLOBALEVENTID" === $"mentions.GLOBALEVENTID", joinType = "left")
      .groupBy($"events.GLOBALEVENTID").count()
      .join(eventsDs, Seq("GLOBALEVENTID"), joinType = "left")
      .orderBy(col("count").desc)


    // Currently to CSV, later to Cassandra
    /*if(fromS3) {
      res.write.mode("overwrite").csv(Context.getS3Path(Context.bucketOutputPath + "reqB_csv"))
    }
    else {
      res.write.mode("overwrite").csv(Context.outputPath + "reqB_csv")
    }

    logger.info("Completed write of request a)")*/
    val columnNames = Seq("GLOBALEVENTID", "SQLDATE", "count")
    res.select(columnNames.map(c => col(c)): _*).rdd.saveToCassandra("test", "queryb", SomeColumns("globaleventid", "sqldate", "count"))


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