

import io.github.embeddedkafka.EmbeddedKafka
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import io.delta.tables.DeltaTable

import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, current_timestamp, from_json, sum}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import scala.concurrent.{ExecutionContext, Future}

object BronzeDeltaApp {
  //kafka
  val topic: String = "test"
  val messageCounter: AtomicInteger = new AtomicInteger()
  val rand: Random = scala.util.Random
  val purchaseCategory: Seq[String] = Seq("technology", "home", "grocery", "fashion", "beauty", "pets", "toys")
  val accountNames: Seq[String] = Seq("John", "James", "Amy", "Arthur", "Lily", "Jane", "Joshua", "Julie")
  val generatedMessageLimit = 100

  //delta
  val bronzeDeltaTableLocation = "C:\\deltapoc\\deltalake\\bronze"
  val bronzeDeltaTableCheckpointLocation = "C:\\deltapoc\\deltalake\\checkpoint\\bronze"


  val spark = SparkSession
    .builder()
    .appName("Streaming")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  //create delta table
  val bronzeTable:DeltaTable = DeltaTable.createIfNotExists(spark)
    .location(bronzeDeltaTableLocation)
    .addColumn("log", StringType, true)
    .addColumn("date", DateType, true)
    .addColumn("hour", IntegerType, true)
    .addColumn("minute", IntegerType, true)
    .addColumn("timestamp", TimestampType, true)
    .partitionedBy("date", "hour","minute")
    .execute()

  def main(args: Array[String]): Unit = {
    startKafka

    println("start reading spark stream from kafka")
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:6001")
      .option("startingOffsets", "earliest")
      .option("subscribe", "test")
      .load().selectExpr("CAST(value AS STRING)")

    val dfbronzetable = df.selectExpr("value as log", "current_date() as date","hour(current_timestamp()) as hour","minute(current_timestamp()) as minute", "current_timestamp() as timestamp")

    println("Starting Streaming Query")
    dfbronzetable.printSchema()

    dfbronzetable
      .writeStream.format("console").outputMode("append").start()

    dfbronzetable
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", bronzeDeltaTableCheckpointLocation)
      .option("path", bronzeDeltaTableLocation)
      .start()

    println("Stream started")
    spark.streams.awaitAnyTermination()
  }

  def startKafka():Unit = {

    implicit val globalExecutionContext: ExecutionContext = ExecutionContext.global

    println("Start kafka producer messages")
    EmbeddedKafka.start()
    //Create separate thread that generates randomized kafka message to embedded kafka every second
    Future {
      while (messageCounter.get()<generatedMessageLimit) {
        sendKafkaMessage()
        Thread.sleep(100L)
      }
    }

  }


  def sendKafkaMessage(): Unit = {
    val message = generateMessage
    println("Sending message: " + message)
    EmbeddedKafka.publishStringMessageToKafka(topic, message)
  }

  def generateMessage(): String = {
    val anomalynumber = rand.nextInt(10000)
    val ctr = messageCounter.incrementAndGet
    val amount = rand.nextInt(10000)
    val timestamp = System.currentTimeMillis
    val category = purchaseCategory(rand.nextInt(purchaseCategory.length ))
    val accountName = accountNames(rand.nextInt(accountNames.length ))

    if(anomalynumber<0){
      "{\"id\":\"" + ctr + "\",\"timestamp\":" + timestamp + ",\"amount\":" + amount + ",\"purchase_category\":\"" + category +  "\",\"account_name\":\"" + accountName + "\", anomaly:Sdk}";
    }
    else{
      "{\"id\":\"" + ctr + "\",\"timestamp\":" + timestamp + ",\"amount\":" + amount + ",\"purchase_category\":\"" + category +  "\",\"account_name\":\"" + accountName + "\"}";
    }

  }

}
