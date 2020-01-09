import java.net._
import java.io._
import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream

import sys.process._
import DataMangling._

object Explore1 extends App {

  def fileDownloader(urlOfFileToDownload: String, fileName: String) = {
    val url = new URL(urlOfFileToDownload)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    connection.connect()

    if (connection.getResponseCode >= 400)
      println("error")
    else
      url #> new File(fileName) !!
  }

  /*
   fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
     "/tmp/data/masterfilelist.txt") // save the list file to local

     fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
       "/tmp/data/masterfilelist-translation.txt") // save the list file to local
     */

  val spark = Context.createSession()
  val sc = spark.sparkContext

  import spark.implicits._

  val monthlyFiles = spark.sqlContext.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv(Context.dataPath + "/masterfilelist-translation.txt").
    withColumnRenamed("_c2", "url").
    filter($"url" contains "/201912").
    repartition(200)

  // .take(5)
  /*
  .foreach(r => {
    //println(URL)
    val URL = r.getAs[String](2)
    val fileName = URL.split("/").last
    val dir = Context.dataPath
    val localFileName = dir + fileName
    fileDownloader(URL, localFileName)
    val localFile = new File(localFileName)
    /* AwsClient.s3.putObject("john-doe-telecom-gdelt2019", fileName, localFile )
       localFile.delete() */
  })
   */
  println("File count for " + Context.refPeriod + " = " + monthlyFiles.count)

  val eventsRDD = sc.binaryFiles(Context.dataPath + "/" + Context.refPeriod + "[0-9]*.export.CSV.zip", 100).
    flatMap { // uncompress zip
      case (name: String, content: PortableDataStream) =>

        val zis = new ZipInputStream(content.open)
        Stream.continually(zis.getNextEntry).
          takeWhile(_ != null).
          flatMap { _ =>
            val br = new BufferedReader(new InputStreamReader(zis))
            Stream.continually(br.readLine()).takeWhile(_ != null)
          }

    }
  //val cachedEvents = eventsRDD.cache // RDD

  eventsRDD.map(_.split("\t")).filter(_.length == 61).map(
    e => Event(
      toInt(e(0)), toInt(e(1)), toInt(e(2)), toInt(e(3)), toDouble(e(4)), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15), e(16), e(17), e(18), e(19), e(20),
      e(21), e(22), e(23), e(24), toInt(e(25)), e(26), e(27), e(28), toInt(e(29)), toDouble(e(30)), toInt(e(31)), toInt(e(32)), toInt(e(33)), toDouble(e(34)), toInt(e(35)), e(36), e(37), e(38), e(39), toDouble(e(40)),
      toDouble(e(41)), e(42), toInt(e(43)), e(44), e(45), e(46), e(47), toDouble(e(48)), toDouble(e(49)), e(50), toInt(e(51)), e(52), e(53), e(54), e(55), toDouble(e(56)), toDouble(e(57)), e(58), toBigInt(e(59)), e(60))

  ).toDS.createOrReplaceTempView("export")
  spark.catalog.cacheTable("export")

  spark.sql(
    """
    SELECT SQLDATE, Actor1Geo_CountryCode, GLOBALEVENTID
    FROM export ORDER BY SQLDATE DESC
    """).write.mode("overwrite").csv(Context.outputPath + "/req1_csv")

  /*
  spark.sql("""
    SELECT SQLDATE, COUNT(*) as nbEvents
    FROM export_translation GROUP BY SQLDATE
    """).write.csv(Context.outputPath + "/req2_csv")

  spark.sql("""
    SELECT SQLDATE, COUNT(*) as nbEvents
    FROM export GROUP BY SQLDATE
    """).write.mode("overwrite").csv(Context.outputPath + "/req3_csv")
   */

  println("Program completed")
}
