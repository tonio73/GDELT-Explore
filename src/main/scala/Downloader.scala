package fr.telecom

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.util.zip.ZipInputStream

import sys.process._
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3._
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.AmazonS3
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

// Handle data download (from GDELT)
object Downloader {
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

  // Download a selection of files, save to local file system, and optionally to S3
  def downloadSelection(selection: Dataset[Row], dir: String = Context.dataPath, s3: AmazonS3 = null, saveToBucket: String = "") = {

    selection.foreach(r => {
      val URL = r.getAs[String](2)
      val fileName = URL.split("/").last
      val localFileName = dir + fileName
      fileDownloader(URL, localFileName)
      if (s3 != null) {
        val localFile = new File(localFileName)
        s3.putObject(saveToBucket, fileName, localFile)
        localFile.delete()
      }
    })
  }

  // Extract Zipped files into a RDD
  def zipsToRdd(spark: SparkSession, filePattern: String, filePath: String = Context.dataPath ): RDD[String] = {

    spark.sparkContext.binaryFiles(filePath + "/" + filePattern, 100).
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
  }
}
