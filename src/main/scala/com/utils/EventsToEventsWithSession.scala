package com.utils

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{last, when}

object EventsToEventsWithSession {

  def convert(events: DataFrame)(implicit spark: SparkSession): DataFrame = {
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
