package com.utils

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{last, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PurchasesAttributionProjection {

  def viaPlainSparkSQL(events: DataFrame, purchases: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowSpec = Window
      .partitionBy($"userId")
      .orderBy($"eventTime")
      .rangeBetween(Window.unboundedPreceding, Window.currentRow)

    val eventsWithSessionId = events
      .withColumn("tmpSessionId", when($"eventType" === "app_open", $"eventId"))
      .withColumn("sessionId", last($"tmpSessionId", ignoreNulls = true).over(windowSpec))
      .drop("tmpSessionId")
      .withColumn("campaignId", last($"attributes.campaign_id", ignoreNulls = true).over(windowSpec))
      .withColumn("channelIid", last($"attributes.channel_id", ignoreNulls = true).over(windowSpec))

    eventsWithSessionId
      .where($"eventType" === "purchase")
      .withColumn("purchaseId", $"attributes.purchase_id")
      .join(purchases, "purchaseId")
      .select(
        "purchaseId", "purchaseTime", "billingCost", "isConfirmed", "sessionId", "campaignId", "channelIid"
      )
  }

  def viaAggregator(events: DataFrame, purchases: DataFrame)(implicit spark: SparkSession): DataFrame = ???

}
