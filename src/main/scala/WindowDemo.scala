import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import  org.apache.spark.sql.functions._

object WindowDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()


    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    import spark.implicits._
    // Split the lines into words
//    val words = lines.as[String].flatMap(_.split(" "))
    val words= lines.as[(String,Timestamp)].flatMap(
  line=> line._1.split(" ").map(word=>(word, line._2))
).toDF("word","timestamp")
    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", "10 minutes", "5 minutes"),
      $"word"
    ).count().orderBy("window")

    // Generate running word count
    val wordCounts = words.groupBy("value").count()
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
