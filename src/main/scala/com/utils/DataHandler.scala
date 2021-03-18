package com.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

trait DataHandler extends DataLoader {

  def toParquetNoPartitioning(implicit spark: SparkSession): Unit = {
    val (events, purchases) = inputFromCsv

    events.write.parquet("src/main/resources/mobile_app_clickstream_parquet_np")
    purchases.write.parquet("src/main/resources/user_purchases_parquet_np")
  }

  def toParquetPartition(implicit spark: SparkSession): Unit = {
    val (events, purchases) = inputFromCsv

    events.write.partitionBy("eventType").parquet("src/main/resources/mobile_app_clickstream_parquet")
    purchases.write.partitionBy("isConfirmed").parquet("src/main/resources/user_purchases_parquet")
  }

  def toParquetPartitionByDate(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val (events, purchases) = inputFromCsv

    events
      .withColumn("eventTimeDate", to_date($"eventTime", "yyyy-mm-dd"))
      .write
      .partitionBy("eventTimeDate")
      .parquet("src/main/resources/mobile_app_clickstream_parquet_time")

    purchases
      .withColumn("purchaseTimeDate", to_date($"purchaseTime", "yyyy-mm-dd"))
      .write
      .partitionBy("purchaseTimeDate")
      .parquet("src/main/resources/user_purchases_parquet_time")
  }

}
