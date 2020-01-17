package fr.telecom

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object MainQueryD extends App {

  // Select files corresponding to reference period (as set in Context.scala)
  val spark = Context.createSession()

  import spark.implicits._

  // Read GDELT compressed CSV (currently from /tmp/data, later from S3)
  val gkgRDD: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "[0-9]*.gkg.csv.zip")

  val gkgDs: Dataset[GKG] = GKG.rddToDs(spark, gkgRDD)

  // println("For %s, number of events = %d, number of mentions = %d".format(Context.refPeriod(), eventsDs.count(), mentionsDs.count()))

  // Request a) afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).
  println("Launch request d)")

  val countries = gkgDs.map(x => x.V2Locations.split(","))

  println(countries.show(20, truncate = false))
                        // .filter(_ != "")

  println(gkgDs.select("V2Locations").show(10, truncate = false))

  println(gkgDs.schema)
  println(gkgDs.head())



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
