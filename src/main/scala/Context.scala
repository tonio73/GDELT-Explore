import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Context {

  // Path to data
  val dataPath = "/tmp/data/" // System.getProperty("user.dir") + "/data"

  val outputPath = "/tmp/bigdata/"

  val refYear = "2019"
  val refMonth = "12"

  def refPeriod : String = {
    refYear + refMonth
  }

  // Create and config a Spark session
  def createSession(): SparkSession = {
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
      "spark.driver-memory" ->  "10g"
    ))

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("io.netty").setLevel(Level.WARN)

    val sparkSession = SparkSession
      .builder
      .config(conf)
      .master("local[5]")
      .appName("TP Spark : Trainer")
      .getOrCreate()

    sparkSession
  }
}

