import org.apache.spark.sql.SparkSession

object StreamingDeduplication {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("StructuredNetworkWordCountWatermarking")
      .getOrCreate()
    val streamingDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()
    // Without watermark using guid column
    val duplicates = streamingDF.dropDuplicates("guid")
    val query = duplicates.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
