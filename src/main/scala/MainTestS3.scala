package fr.telecom

import java.io.File
import java.util.Calendar
import java.text.SimpleDateFormat

import com.amazonaws.{AmazonServiceException, SdkClientException}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest

// Test access to S3
// Ref:
// - https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/java/example_code/s3/src/main/java/UploadObject.java
object MainTestS3 extends App {

  try {
    val s3Client = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_1)
      .build();

    // Upload a text string as a new object.
    val now = Calendar.getInstance().getTime()
    val format = new SimpleDateFormat("yyyy-MM-dd-HHmmss")
    s3Client.putObject(Context.bucketName, format.format(now), "Uploaded String Object");


    // Upload a file as a new object with ContentType and title specified.// Upload a file as a new object with ContentType and title specified.
    val request = new PutObjectRequest(Context.bucketName, format.format(now) + "b", new File("/etc/aliases"))
    val metadata = new ObjectMetadata
    metadata.setContentType("plain/text")
    metadata.addUserMetadata("x-amz-meta-title", "someTitle")
    request.setMetadata(metadata)
    s3Client.putObject(request)
  } catch {
    // The call was transmitted successfully, but Amazon S3 couldn't process
    // it, so it returned an error response.
    case e: AmazonServiceException => e.printStackTrace();
    // Amazon S3 couldn't be contacted for a response, or the client
    // couldn't parse the response from Amazon S3.
    case e: SdkClientException => e.printStackTrace();
  }
}
