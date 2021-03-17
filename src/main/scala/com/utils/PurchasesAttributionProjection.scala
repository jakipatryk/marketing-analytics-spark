package com.utils

import com.utils.models.Event
import org.apache.spark.sql.{DataFrame, SparkSession}

object PurchasesAttributionProjection {

  def viaPlainSparkSQL(events: DataFrame, purchases: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val eventsWithSessionId = EventsToEventsWithSession.convert(events)

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

}
