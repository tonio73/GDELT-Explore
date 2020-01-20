package fr.telecom

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._

/*
Ressources :
- [1] https://www.datastax.com/blog/2015/01/kindling-introduction-spark-cassandra-part-1
- [2] https://aws.amazon.com/fr/premiumsupport/knowledge-center/emr-timeout-connection-wait/
 */

object Context {

  // Path to data
  val dataPath = "/tmp/data/" // System.getProperty("user.dir") + "/data"

  val outputPath = "/tmp/bigdata/"

  val bucketName = "fufu-program"

  val bucketDataPath = "data/"

  val bucketOutputPath = "bigdata/"

  val refYear = "2019"
  val refMonth = "01"
  val refDay = "01"
  // val refMonth = "12"
  // val refDay   = "15"

  def refPeriod(sep: String = ""): String = {
    if (refDay.isEmpty)
      Seq(refYear, refMonth).mkString(sep)
    else
      Seq(refYear, refMonth, refDay).mkString(sep)
  }

  // Create and config a Spark session
  def createSession(localMaster: Boolean, cassandraServerIp: String = ""): SparkSession = {
    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.driver.maxResultSize" -> "2g",
      "spark.driver-memory" -> "10g"
    ))

    if (!cassandraServerIp.isEmpty()) {
      conf.set("spark.cassandra.connection.host", cassandraServerIp)
    }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("io.netty").setLevel(Level.WARN)

    val sparkSessionBuilder = SparkSession.builder
      .config(conf)
      .appName("GDELT-ETL")

    if (localMaster) {
      sparkSessionBuilder.master("local[4]")
    }

    val spark = sparkSessionBuilder.getOrCreate()

    // Increase number of max connections to S3 [2]
    val hc = spark.sparkContext.hadoopConfiguration
    hc.setInt("fs.s3.maxConnections", 1000)

    spark
  }

  def getS3(): AmazonS3 = {
    AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_1)
      .build()
  }

  def getS3Path(subPath: String) = {
    "s3://" + Context.bucketName + "/" + subPath
  }

  // Log43 logger for the application
  def logger() = {
    Logger.getLogger(this.getClass.getPackage.getName)
  }
}

