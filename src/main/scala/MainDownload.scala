package fr.telecom

import org.apache.spark.sql.{Dataset, Row}

object MainDownload {

  def main(args: Array[String]): Unit = {

    var downLoadIndex = false

    // Command line args
    var i: Int = 0
    while (i < args.length) {
      args(i) match {
        case "--index" => downLoadIndex = true

        case "target.*\\.jar" => {/* ignore */}

        case _ => {
          print("Unknown argument " + args(i) + "\n")
          print("Usage: --index to download master files\n")
        }
      }
    }

    if (downLoadIndex) {
      // Download indexes (master files) to /tmp/data

      Downloader.fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
        Context.dataPath + "/masterfilelist.txt") // save the list file to local

    //  Downloader.fileDownloader("http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt",
    //    Context.dataPath + "/masterfilelist-translation.txt") // save the list file to local
    }

    // Select files corresponding to reference period (as set in Context.scala)
    val spark = Context.createSession()

    import spark.implicits._

    // List all files related to the reference period from the master file
    val selectedFiles: Dataset[Row] = spark.sqlContext.read.
      option("delimiter", " ").
      option("infer_schema", "true").
      csv(Context.dataPath + "/masterfilelist.txt").
      withColumnRenamed("_c2", "url").
      // Filter on period
      filter($"url" contains "/" + Context.refPeriod()).
      repartition(200)

    println("File count for " + Context.refPeriod("-") + " = " + selectedFiles.count)

    // Download all files related to the reference period
    println("Download all files related to the reference period")
    Downloader.downloadSelection(selectedFiles)
  }
}
