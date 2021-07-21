
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, count, current_timestamp, from_json, from_unixtime, sum, to_timestamp, to_utc_timestamp, hour,minute,to_date}
object SilverDeltaApp {
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

  val bronzeDeltaTableLocation = "C:\\deltapoc\\deltalake\\bronze"
  val silverDeltaTableLocation = "C:\\deltapoc\\deltalake\\silver"

  val silverDeltaTable: DeltaTable = DeltaTable.createIfNotExists(spark)
    .location(silverDeltaTableLocation)
    .addColumn("id", StringType, true)
    .addColumn("timestamp", TimestampType, true)
    .addColumn("date", DateType, true)
    .addColumn("hour", IntegerType, true)
    .addColumn("minute", IntegerType, true)
    .addColumn("amount", IntegerType, true)
    .addColumn("purchase_category", StringType, true)
    .addColumn("account_name", StringType, true)
    .partitionedBy("date", "hour","minute","purchase_category")
    .execute()

  def upsertToSilverTable(microBatchOutputDF: DataFrame, batchId: Long) {
    val startTime = System.currentTimeMillis()
    val json_log_df = spark.read.json(microBatchOutputDF.select("log").as(Encoders.STRING))
      .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp")/1000,"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("date", to_date(col("timestamp")))
      .withColumn("hour", hour(col("timestamp")))
      .withColumn("minute", minute(col("timestamp")))
      .dropDuplicates("id") //to prevent multiple sources to merge to target we must drop duplicates, alternative is to rank latest duplicate row and order them by latest timestamp
    println("Batch:" + batchId + " - Upsert to silver")

    if(json_log_df.columns.contains("_corrupt_record")){
      val filter_corrupt_df = json_log_df.filter("_corrupt_record is null")
      filter_corrupt_df.printSchema()
      silverDeltaTable.as("silver")
        .merge(
          filter_corrupt_df.as("bronze"),
          "bronze.id = silver.id")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }
    else{
      json_log_df.printSchema()
      silverDeltaTable.as("silver")
        .merge(
          json_log_df.as("bronze"),
          "bronze.id = silver.id")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }
    val endTime = System.currentTimeMillis()
    val diffInSeconds = (endTime - startTime)/1000
    println("Batch:" + batchId + " - Upsert Done - Time Elapsed : " + diffInSeconds + " seconds" )
  }



  def main(args: Array[String]): Unit = {
    stream()
  }

  def stream(): Unit = {
    print("Starting Stream")
//    spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled","true")
    val deltaStreamDF = spark.readStream.format("delta").option("ignoreChanges","true").load(bronzeDeltaTableLocation)

    deltaStreamDF.writeStream.format("console").start()
    deltaStreamDF.writeStream.format("delta").foreachBatch(upsertToSilverTable _).start()
    print("Stream started")
    spark.streams.awaitAnyTermination()
  }


  def batch(): Unit = {
    val deltaBatchDF = spark.read.format("delta").option("ignoreChanges","true").load(bronzeDeltaTableLocation)
    deltaBatchDF.show()
    val dfcount = deltaBatchDF.count()
    println(dfcount)

    val json_log_df = spark.read.json(deltaBatchDF.select("log").as(Encoders.STRING))
    json_log_df.printSchema()
    json_log_df.show()
    if(json_log_df.columns.contains("_corrupt_record")){
      println("Has corrupt records")
      json_log_df.groupBy("_corrupt_record").count().show()
      println("_corrupt_record filter !=null : " + json_log_df.filter("_corrupt_record is not null").count() )
      println("_corrupt_record filter ==null : " + json_log_df.filter("_corrupt_record is null").count() )

    }

  }
}
