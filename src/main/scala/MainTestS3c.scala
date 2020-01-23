package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}
import org.apache.spark.rdd.RDD

object MainTestS3c extends App {
  val logger = Context.logger

  try {

    var localMaster = false
    var i: Int = 0
    while (i < args.length) {
      args(i) match {
        case "--local-master" => localMaster = true

        case _ => {
          print("Unknown argument " + args(i) + "\n")
          print("Usage: --index to download master files\n")
        }
      }
      i += 1
    }

    val spark = Context.createSession("GDELT-ETL-TestS3c", localMaster)

    logger.info("Spark session created")

    // Following is only ok on AWS as specific configuration of Hadoop filesystem is required
    val eventsRdd: RDD[String] = Downloader.zipsToRdd(spark, Context.refPeriod() + "*.export.CSV.zip", true)

    eventsRdd.take(1)

  } catch {
    // The call was transmitted successfully, but Amazon S3 couldn't process
    // it, so it returned an error response.
    case e: AmazonServiceException => e.printStackTrace();
    // Amazon S3 couldn't be contacted for a response, or the client
    // couldn't parse the response from Amazon S3.
    case e: SdkClientException => e.printStackTrace();
  }

  logger.info("Program completed")
}
