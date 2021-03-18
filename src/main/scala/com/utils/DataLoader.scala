package com.utils

import org.apache.spark.sql.functions.{from_json, regexp_extract, regexp_replace}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataLoader {

  def inputFromCsv(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    loadCsv(
      "src/main/resources/mobile_app_clickstream",
      "src/main/resources/user_purchases"
    )
  }

  def inputFromParquetNoPartitioning(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    loadParquet(
      "src/main/resources/mobile_app_clickstream_parquet_np",
      "src/main/resources/user_purchases_parquet_np"
    )
  }

  def inputFromParquetPartitionedByEventTimeAndIsConfirmed(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    loadParquet(
      "src/main/resources/mobile_app_clickstream_parquet",
      "src/main/resources/user_purchases_parquet"
    )
  }

  def inputFromParquetPartitionedByDate(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    loadParquet(
      "src/main/resources/mobile_app_clickstream_parquet_time",
      "src/main/resources/user_purchases_parquet_time"
    )
  }

  private def loadCsv(eventsPath: String, purchasesPath: String)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._

    val events = spark
      .read
      .option("header", "true")
      .csv(eventsPath)
      .withColumn("eventTime", $"eventTime".cast(TimestampType))
      .withColumn("attributes", regexp_extract($"attributes", "\"?(\\{.+\\})\"?", 1))
      .withColumn("attributes", regexp_replace($"attributes", "\"\"", "'"))
      .withColumn("attributes", from_json($"attributes", MapType(StringType, StringType)))

    val purchasesSchema = StructType(
      StructField("purchaseId", StringType)
        :: StructField("purchaseTime", TimestampType)
        :: StructField("billingCost", DoubleType)
        :: StructField("isConfirmed", BooleanType)
        :: Nil
    )
    val purchases = spark
      .read
      .option("header", "true")
      .schema(purchasesSchema)
      .csv(purchasesPath)

    (events, purchases)
  }

  private def loadParquet(eventsPath: String, purchasesPath: String)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    val events = spark.read.parquet(eventsPath)
    val purchases = spark.read.parquet(purchasesPath)
    (events, purchases)
  }

}
