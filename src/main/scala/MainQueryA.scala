package fr.telecom

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object MainQueryA extends App {

  // Select files corresponding to reference period (as set in Context.scala)
  val spark = Context.createSession()

  import spark.implicits._

  // Read GDELT compressed CSV (currently from /tmp/data, later from S3)
  val eventsRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "[0-9]*.export.CSV.zip")
  val mentionsRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "[0-9]*.mentions.CSV.zip")

  val eventsDs: Dataset[Event] = Event.rddToDs(spark, eventsRDD)
  val mentionsDs: Dataset[Mention] = Mention.rddToDs(spark, mentionsRDD)

  // println("For %s, number of events = %d, number of mentions = %d".format(Context.refPeriod(), eventsDs.count(), mentionsDs.count()))

  // Request a) afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).
  println("Launch request a)")

  val reqA = eventsDs.as("events").join(mentionsDs.as("mentions"), $"events.GLOBALEVENTID" === $"mentions.GLOBALEVENTID", joinType="left" ).
    groupBy("SQLDATE", "ActionGeo_CountryCode", "SRCLC").
    count()

  // Currently to CSV, later to Cassandra
  reqA.write.mode("overwrite").csv(Context.outputPath + "/reqA_csv")

  println("Completed write of request a)")

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
