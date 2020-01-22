package fr.telecom

import com.datastax.spark.connector._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Uploader {

  def persistDataFrame(fromS3: Boolean, cassandraIp: String, df: DataFrame, saveCols : Seq[String],
                       csvName: String,
                       keySpace: String, table:String, cassandraCols: SomeColumns) ={
    // Write
    if (cassandraIp.isEmpty) {
      // Default to CSV write either to S3 or local tmp
      if (fromS3) {
        df.write.mode("overwrite").csv(Context.getS3Path(Context.bucketOutputPath + csvName))
      }
      else {
        df.write.mode("overwrite").csv(Context.outputPath + csvName)
      }
    }
    else {
      // Save to Cassandra
      df.select(saveCols.map(c => col(c)): _*).rdd.saveToCassandra(keySpace, table, cassandraCols)
    }
  }

}
