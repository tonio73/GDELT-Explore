package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}
import org.apache.spark.sql.{Dataset, Row}

object MainDownload {

  def main(args: Array[String]): Unit = {

    val logger = Context.logger

    // Command line args
    var downloadIndex = false
    var downloadTranslations = false
    var downloadData = true
    var saveToS3 = false
    var localMaster = false
    var refPeriod = Context.refPeriod()
    var i: Int = 0
    while (i < args.length) {
      args(i) match {
        case "--index" => downloadIndex = true
        case "--index-only" => {
          downloadIndex = true
          downloadData = false
        }

        case "--translations" => downloadTranslations = true

        case "--local-master" => localMaster = true

        case "--to-s3" => saveToS3 = true

        case "--ref-period" => {
          i += 1
          refPeriod = args(i)
        }

        case _ => {
          print("Unknown argument " + args(i) + "\n")
          print("Usage: --index to download master files\n")
          System.exit(1)
        }
      }
      i += 1
    }


    try {

      if (downloadIndex) {
        // Download indexes (master files) to /tmp/data

        logger.info("Start downloading materfiles")

        // save the list file to local
        Downloader.fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
          "masterfilelist.txt", saveToS3)

        if (downloadTranslations) {
          Downloader.fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
            "masterfilelist-translation.txt", saveToS3)
        }

        logger.info("End downloading materfiles")
      }

      val masterFilePath = if (saveToS3) Context.getS3Path(Context.bucketDataPath)
      else Context.dataPath

      if (downloadData) {

        logger.info("Setup spark session")

        // Select files corresponding to reference period (as set in Context.scala)
        val spark = Context.createSession("GDELT-ETL-MainDownload", localMaster)

        import spark.implicits._

        // List all files related to the reference period from the master file
        val selectedMasterFiles: Dataset[Row] = spark.sqlContext.read.
          option("delimiter", " ").
          option("infer_schema", "true").
          csv(masterFilePath + "/masterfilelist.txt").
          withColumnRenamed("_c2", "url").
          // Filter on period
          filter($"url" contains "/" + refPeriod).
          repartition(200)

        val selectedFiles = if (downloadTranslations)
          selectedMasterFiles.union(spark.sqlContext.read.
            option("delimiter", " ").
            option("infer_schema", "true").
            csv(masterFilePath + "/masterfilelist-translation.txt").
            withColumnRenamed("_c2", "url").
            // Filter on period
            filter($"url" contains "/" + refPeriod))
        else
          selectedMasterFiles

        logger.info("Start downloading selection on reference period %s, total count = %d".format(refPeriod, selectedFiles.count))

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
