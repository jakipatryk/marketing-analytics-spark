package com.utils

import com.utils.InputType._

import org.apache.spark.sql.functions.{from_json, regexp_extract, regexp_replace}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataLoader {

  def loadInput(implicit spark: SparkSession, config: Config): (DataFrame, DataFrame) = config.inputType match {
    case Csv => loadCsv
    case Parquet => loadParquet
  }

  private def loadCsv(implicit spark: SparkSession, config: Config): (DataFrame, DataFrame) = {
    import spark.implicits._

    val events = spark
      .read
      .option("header", "true")
      .csv(config.eventsInputPath)
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
      .csv(config.purchasesInputPath)

    (events, purchases)
  }

  private def loadParquet(implicit spark: SparkSession, config: Config): (DataFrame, DataFrame) = {
    val events = spark.read.parquet(config.eventsInputPath)
    val purchases = spark.read.parquet(config.purchasesInputPath)
    (events, purchases)
  }

}
