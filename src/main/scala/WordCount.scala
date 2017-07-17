import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._
    //lines DataFrame is the input table,
    //read text from socket
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    println(lines.isStreaming)// Returns True for DataFrames that have streaming sources
    lines.printSchema()
    // Read all the csv files written atomically in a directory
//    val userSchema=new StructType().add("name","string").add("age","integer")
//
//    val lines=spark.readStream
//      .option("sep",":")
//      .schema(userSchema)
//      .csv("D:/a.csv")
    val words = lines.as[String].flatMap(_.split(" "))
    //wordCounts DataFrame is the result table
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()

  }
}
