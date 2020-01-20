package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}
import org.apache.spark.sql.{Dataset, Row}

object MainDownload {

  def main(args: Array[String]): Unit = {

    val logger = Context.logger

    // Command line args
    var downloadIndex = false
    var downloadData = true
    var saveToS3 = false
    var i: Int = 0
    while (i < args.length) {
      args(i) match {
        case "--index" => downloadIndex = true
        case "--index-only" => {
          downloadIndex = true
          downloadData = false
        }

        case "--to-s3" => saveToS3 = true

        case _ => {
          print("Unknown argument " + args(i) + "\n")
          print("Usage: --index to download master files\n")
        }
      }
      i += 1
    }


    try {

      if (downloadIndex) {
        // Download indexes (master files) to /tmp/data

        logger.info("Start downloading materfilelist.txt")

        // save the list file to local
        val msPath = Downloader.fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
          "masterfilelist.txt", saveToS3)

        logger.info("End downloading materfilelist.txt")

        //  Downloader.fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
        //    Context.dataPath + "/masterfilelist-translation.txt") // save the list file to local
        msPath
      }

      val masterFilePath = if (saveToS3) "s3://" + Context.bucketName + "/" + Context.bucketDataPath + "/masterfilelist.txt"
      else Context.dataPath + "/masterfilelist.txt"

      if (downloadData) {

        logger.info("Setup spark session")

        // Select files corresponding to reference period (as set in Context.scala)
        val spark = Context.createSession()

        import spark.implicits._

        // List all files related to the reference period from the master file
        val selectedFiles: Dataset[Row] = spark.sqlContext.read.
          option("delimiter", " ").
          option("infer_schema", "true").
          csv(masterFilePath).
          withColumnRenamed("_c2", "url").
          // Filter on period
          filter($"url" contains "/" + Context.refPeriod()).
          repartition(200)

        logger.info("Start downloading selection on reference period %s, total count = %d".format(Context.refPeriod("-"), selectedFiles.count))

        // Download all files related to the reference period
        Downloader.downloadSelection(selectedFiles, saveToS3)

        logger.info("End downloading selection")
      }
    }
    catch {
      // The call was transmitted successfully, but AWS couldn't process
      // it, so it returned an error response.
      case e: AmazonServiceException => e.printStackTrace();
      // AWS couldn't be contacted for a response, or the client
      // couldn't parse the response from Amazon S3.
      case e: SdkClientException => e.printStackTrace();
    }

    logger.info("Fu-fu program completed")
  }
}
