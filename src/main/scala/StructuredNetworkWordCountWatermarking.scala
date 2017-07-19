import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.functions._

object StructuredNetworkWordCountWatermarking {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("StructuredNetworkWordCountWatermarking")
      .getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()
    import spark.implicits._
    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")
    val windowedCounts=words
      .withWatermark("timestamp","10minutes")
      .groupBy(
        window($"timestamp","10 minutes","5 minutes"),
        $"word")
      .count().orderBy($"window")
    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
