package fr.telecom

import java.io.{BufferedReader, File, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.util.zip.ZipInputStream

import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.reflect.io.Directory
import scala.sys.process._

// Handle data download (from GDELT)
object Downloader {
  def fileDownloader(urlOfFileToDownload: String, fileName: String, saveToS3: Boolean) = {

    val url = new URL(urlOfFileToDownload)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    connection.connect()

    if (connection.getResponseCode >= 400) {
      throw new Exception("Unable to download from URL: " + urlOfFileToDownload)
    }
    else {
      val contentLength = connection.getContentLength

      if (contentLength <= 0) {
        throw new Exception("Unable to get content length for URL:" + urlOfFileToDownload)
      }
      else {

        if (saveToS3) {
          val getStream = new URL(urlOfFileToDownload).openStream()
          val metadata = new ObjectMetadata
          metadata.setContentLength(contentLength)
          Context.getS3().putObject(Context.bucketName, Context.bucketDataPath + fileName, getStream, metadata)
        } else {
          Downloader.checkPath(Context.dataPath) // Must check on each worker !
          url #> new File(Context.dataPath + fileName) !!
        }
      }
    }
  }

  // Download a selection of files, save to local file system, and optionally to S3
  def downloadSelection(selection: Dataset[Row], saveToS3: Boolean) = {

    selection.foreach(r => {
      val URL = r.getAs[String](2)
      val fileName = URL.split("/").last
      fileDownloader(URL, fileName, saveToS3)
    })
  }

  // Extract Zipped files into a RDD
  def zipsToRdd(spark: SparkSession, filePattern: String, fromS3: Boolean): RDD[String] = {

    var path = if (fromS3) Context.getS3Path(Context.bucketDataPath) else Context.dataPath

    spark.sparkContext.binaryFiles(path + filePattern).
      flatMap { // uncompress zip
        case (name: String, content: PortableDataStream) =>

          val zis = new ZipInputStream(content.open)
          Stream.continually(zis.getNextEntry).
            takeWhile {
              case null => zis.close(); false
              case _ => true
            }.
            flatMap {
              _ =>
                val br = new BufferedReader(new InputStreamReader(zis))
                Stream.continually(br.readLine()).takeWhile(_ != null)
            }
      }
  }

  // Check path and create if it does not exist
  def checkPath(path: String) = {
    val directory = Directory(path)

    if (!directory.exists) {
      directory.createDirectory()
    }
  }
}
