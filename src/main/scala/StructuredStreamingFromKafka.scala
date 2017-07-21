import org.apache.spark.sql.SparkSession

object StructuredStreamingFromKafka {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("StructuredStreamingFromKafka")
      .getOrCreate()
    //Subscribe to 1 topic
    val ds1= spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","host1:port1,host2:port2")
      .option("subscribe","topic1")
      .load()
    ds1.selectExpr("CAST(key AS STRINNG)","CAST(value AS STRING)")
      .as[(String,String)]
    //Subscribe to multiple topics
    val ds2= spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","host1port1,host2:port2")
      .option("subscribe","topic1,topic2")
      .load()
    ds2.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
    //Subscribe to a pattern
    val ds3=spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","host1:port1,host2:port2")
      .option("subscribePattern","topic.*")
      .load()
    ds3.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
  }

}
