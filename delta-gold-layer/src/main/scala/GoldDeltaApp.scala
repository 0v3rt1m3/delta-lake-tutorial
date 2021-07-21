
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, count, current_timestamp, from_json, sum}

object  GoldDeltaApp {
  val silverDeltaTableLocation = "C:\\deltapoc\\deltalake\\silver"

  val spark = SparkSession
    .builder()
    .appName("Streaming")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.databricks.delta.merge.repartitionBeforeWrite.enabled","true")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()

  val silverTableDf = spark.read.format("delta").load(silverDeltaTableLocation)

  val silverTableDfAgg = silverTableDf.groupBy("account_name")
                                      .agg( count("account_name").as("transaction_count"), sum("amount").as("total_amount"))
  def displayAggregatesForeach(microBatchOutputDF: DataFrame, batchId: Long): Unit = {
    println("Batch "  + batchId)
    silverTableDfAgg.show()
  }
  def main(args: Array[String]): Unit = {
    stream()
  }

  def batch(): Unit = {

    val deltaDF = spark.read.format("delta").option("ignoreChanges","true").load(silverDeltaTableLocation)
    deltaDF.printSchema()
    deltaDF.show()
  }



  def stream(): Unit = {
    println("Starting Stream")
    val deltaStreamDF = spark.readStream.format("delta").option("ignoreChanges","true").load(silverDeltaTableLocation)
    //Aggregate count and sum per account name
//    deltaStreamDF
//      .groupBy("account_name")
//      .agg( count("account_name").as("transaction_count"), sum("amount").as("total_amount"))
//      .writeStream.format("console")
//      .outputMode("complete")
//      .start()

    //Aggregate count and sum per category
//    deltaStreamDF
//      .groupBy("purchase_category")
//      .agg( count("purchase_category").as("transaction_count"), sum("amount").as("total_amount"))
//      .writeStream.format("console")
//      .outputMode("complete")
//      .start()

    //Stream aggregates on foreach
    deltaStreamDF
        .writeStream
        .foreachBatch(displayAggregatesForeach _)
        .start()

    println("Stream started")
    spark.streams.awaitAnyTermination()
  }
}
