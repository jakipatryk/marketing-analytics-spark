package com.utils

import org.apache.spark.sql.functions.{from_json, regexp_extract, regexp_replace}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataLoader {

  def loadCsv(eventsPath: String, productsPath: String)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
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
      .csv(productsPath)

    (events, purchases)
  }

  def loadParquet(eventsPath: String, productsPath: String)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    ???
  }

}
