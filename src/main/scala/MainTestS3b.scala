package fr.telecom

import com.amazonaws.{AmazonServiceException, SdkClientException}

// Test access to S3
// Ref:
// - https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/java/example_code/s3/src/main/java/UploadObject.java
object MainTestS3b extends App {

  val logger = Context.logger

  try {
    val urlStr = "https://www.gstatic.com/images/branding/googlelogo/2x/googlelogo_color_92x36dp.png"
    val fileName = "google.png"


    Downloader.fileDownloader(urlStr, fileName, true)

  } catch {
    // The call was transmitted successfully, but Amazon S3 couldn't process
    // it, so it returned an error response.
    case e: AmazonServiceException => e.printStackTrace();
    // Amazon S3 couldn't be contacted for a response, or the client
    // couldn't parse the response from Amazon S3.
    case e: SdkClientException => e.printStackTrace();
  }
}
