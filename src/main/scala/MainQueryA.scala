package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object MainQueryA extends App {

  val logger = Context.logger

  try {

    var localMaster = false
    var fromS3 = false
    var i: Int = 0
    while (i < args.length) {
      args(i) match {
        case "--local-master" => localMaster = true

        case "--from-s3" => fromS3 = true

        case _ => {
          print("Unknown argument " + args(i) + "\n")
          print("Usage: --index to download master files\n")
        }
      }
      i += 1
    }

    logger.info("Create Spark session")

    // Select files corresponding to reference period (as set in Context.scala)
    val spark = Context.createSession(localMaster)

    import spark.implicits._

    // Read GDELT compressed CSV (currently from /tmp/data, later from S3)
    val eventsRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "*.export.CSV.zip", fromS3)
    val mentionsRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "*.mentions.CSV.zip", fromS3)

    val eventsDs: Dataset[Event] = Event.rddToDs(spark, eventsRDD)
    val mentionsDs: Dataset[Mention] = Mention.rddToDs(spark, mentionsRDD)

    // println("For %s, number of events = %d, number of mentions = %d".format(Context.refPeriod(), eventsDs.count(), mentionsDs.count()))

    // Request a) afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).
    logger.info("Launch request a)")

    /* GDELT Codebook : "It also makes it possible to identify the “best” news report to return for a given event
        (filtering all mentions of an event for those with the highest Confidence scores, most prominent positioning
        within the article, and/or in a specific source language – such as Arabic coverage of a protest versus English coverage of that protest)."
     */
    // Using Window function to extract the mention with the best confidence
    // https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
    // https://sparkbyexamples.com/spark/spark-dataframe-how-to-select-the-first-row-of-each-group/
    val w2 = Window.partitionBy("GLOBALEVENTID").orderBy(desc("Confidence"))
    val bestMentionsDs = mentionsDs.withColumn("row", row_number.over(w2))
      .where($"row" === 1).drop("row")

    val reqA = eventsDs.as("events").join(bestMentionsDs.as("mentions"),
      $"events.GLOBALEVENTID" === $"mentions.GLOBALEVENTID",
      joinType = "left").
      groupBy("SQLDATE", "ActionGeo_CountryCode", "SRCLC").
      count()

    // Currently to CSV, later to Cassandra
    if(fromS3) {
      reqA.write.mode("overwrite").csv(Context.getS3Path(Context.bucketOutputPath + "reqA_csv"))
    }
    else {
      reqA.write.mode("overwrite").csv(Context.outputPath + "reqA_csv")
    }

    logger.info("Completed write of request a)")

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

     */
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
