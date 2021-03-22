package com.utils

import com.utils.models.Event
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{last, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PurchasesAttributionProjection {

  def viaPlainSparkSQL(events: DataFrame, purchases: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val eventsWithSessionId = includeSessionId(events)

    eventsWithSessionId
      .where($"eventType" === "purchase")
      .withColumn("purchaseId", $"attributes.purchase_id")
      .join(purchases, "purchaseId")
      .select(
        "purchaseId", "purchaseTime", "billingCost", "isConfirmed", "sessionId", "campaignId", "channelIid"
      )
  }

  def viaAggregator(events: DataFrame, purchases: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // TODO: find a way to use DF here instead of DS (should be possible since it is Spark 3)
    events
      .as[Event]
      .groupByKey(_.userId)
      .agg(PurchasesWithSessionAggregator.toColumn)
      .flatMap(_._2)
      .join(purchases, "purchaseId")
      .select(
        "purchaseId", "purchaseTime", "billingCost", "isConfirmed", "sessionId", "campaignId", "channelIid"
      )
  }

  private def includeSessionId(events: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowSpec = Window
      .partitionBy($"userId")
      .orderBy($"eventTime")
      .rangeBetween(Window.unboundedPreceding, Window.currentRow)

    events
      .withColumn("tmpSessionId", when($"eventType" === "app_open", $"eventId"))
      .withColumn("sessionId", last($"tmpSessionId", ignoreNulls = true).over(windowSpec))
      .drop("tmpSessionId")
      .withColumn("campaignId", last($"attributes.campaign_id", ignoreNulls = true).over(windowSpec))
      .withColumn("channelIid", last($"attributes.channel_id", ignoreNulls = true).over(windowSpec))
  }

}
