import java.net._
import java.io._
import sys.process._

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

  import spark.implicits._

  spark.sqlContext.read.
    option("delimiter", " ").
    option("infer_schema", "true").
    csv(Context.dataPath + "/masterfilelist-translation.txt").
    withColumnRenamed("_c2", "url").
    filter(  $"url" contains "/201912").
    repartition(200)
    // .take(5)
    .foreach(r => {
      //println(URL)
      val URL = r.getAs[String](2)
      val fileName = URL.split("/").last
      val dir = Context.outputPath
      val localFileName = dir + fileName
      fileDownloader(URL, localFileName)
      val localFile = new File(localFileName)
      /* AwsClient.s3.putObject("john-doe-telecom-gdelt2019", fileName, localFile )
         localFile.delete() */
    })
}
