package fr.telecom

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._

object MainQueryB extends App {
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
  println("Launch request b)")
  val country = "FR"

  // First data to put into cassandra, the country parameter must be given to cassandra and not spark (need to do filter in cassandra)
  val res = eventsDs.as("events").join(mentionsDs.as("mentions"), $"events.GLOBALEVENTID" === $"mentions.GLOBALEVENTID", joinType = "left")
    .groupBy($"events.GLOBALEVENTID").count()
    .join(eventsDs, Seq("GLOBALEVENTID"), joinType = "left")
    .orderBy(col("count").desc)



  // Currently to CSV, later to Cassandra
  // res.write.mode("overwrite").csv(Context.outputPath + "/reqB_csv")
  val columnNames = Seq("GLOBALEVENTID", "SQLDATE", "count")
  res.select(columnNames.map(c => col(c)): _*).rdd.saveToCassandra("test", "queryb", SomeColumns("globaleventid", "sqldate", "count"))

  println("Finish request b)")
}