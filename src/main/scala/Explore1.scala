package fr.telecom

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

object Explore1 extends App {

  if (false) {
    Downloader.fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
      Context.dataPath + "/masterfilelist.txt") // save the list file to local

    Downloader.fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
      Context.dataPath + "/masterfilelist-translation.txt") // save the list file to local
  }

  val spark = Context.createSession()

  import spark.implicits._

  // List all files related to the reference period from the master file
  val selectedFiles: Dataset[Row] = spark.sqlContext.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv(Context.dataPath + "/masterfilelist.txt").
    withColumnRenamed("_c2", "url").
    // Filter on period
    filter($"url" contains "/" + Context.refPeriod()).
    repartition(200)

  println("File count for " + Context.refPeriod("-") + " = " + selectedFiles.count)

  /*
    // Download all files related to the reference period
    println("Download all files related to the reference period")
    Downloader.downloadSelection(selectedFiles)
  */

  val eventsRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "[0-9]*.export.CSV.zip")
  val mentionsRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "[0-9]*.mentions.CSV.zip")
  //val cachedEvents = eventsRDD.cache // RDD

  val eventsDs: Dataset[Event] = Event.rddToDs(spark, eventsRDD)
  val mentionsDs: Dataset[Mention] = Mention.rddTods(spark, mentionsRDD)

  // println("For %s, number of events = %d, number of mentions = %d".format(Context.refPeriod(), eventsDs.count(), mentionsDs.count()))

  // Request a) afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).
  println("Launch request a)")

  val reqA = eventsDs.as("events").join(mentionsDs.as("mentions"), $"events.GLOBALEVENTID" === $"mentions.GLOBALEVENTID", joinType="left" ).
    // distinct().
    groupBy("SQLDATE", "ActionGeo_CountryCode", "SRCLC").
    count()

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
