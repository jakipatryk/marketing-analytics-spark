package com.utils

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, first, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CampaignsAndChannelsStatistics {

  def topCampaignsByRevenue(purchasesAttribution: DataFrame, n: Int = 10)(implicit spark: SparkSession): DataFrame = {
    purchasesAttribution.createOrReplaceTempView("purchasesAttribution")

    val query =
      s"""SELECT campaignId, SUM(billingCost) as revenue
        |FROM purchasesAttribution
        |WHERE isConfirmed = true
        |GROUP BY campaignId
        |ORDER BY revenue DESC
        |LIMIT $n
        |""".stripMargin

    spark.sql(query)
  }

  def topCampaignsByRevenueWithoutSQL(purchasesAttribution: DataFrame, n: Int = 10)
                                     (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    purchasesAttribution
      .where($"isConfirmed" === true)
      .groupBy($"campaignId")
      .agg(sum($"billingCost") as "revenue")
      .orderBy($"revenue".desc)
      .limit(n)
  }

  def mostPopularChannelsInCampaigns(events: DataFrame)(implicit spark: SparkSession): DataFrame = {
    events.createOrReplaceTempView("events")

    val query =
      """SELECT DISTINCT campaignId,
        | FIRST(channelIid) OVER (PARTITION BY campaignId ORDER BY COUNT(eventId) DESC) as channelIiD
        |FROM (
        | SELECT eventId, attributes.campaign_id AS campaignId, attributes.channel_id AS channelIid
        | FROM events
        | WHERE eventType = "app_open"
        |)
        |GROUP BY campaignId, channelIid
        |""".stripMargin

    spark.sql(query)
  }

  def mostPopularChannelsInCampaignsWithoutSQL(events: DataFrame)
                                              (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val appOpenEvents = events
      .where($"eventType" === "app_open")
      .select($"eventId", $"attributes.campaign_id" as "campaignId", $"attributes.channel_id" as "channelIid")

    val windowSpec = Window
      .partitionBy($"campaignId")
      .orderBy(count($"eventId").desc)

    appOpenEvents
      .groupBy("campaignId", "channelIid")
      .agg(first("channelIid") over windowSpec as "mostPopularChannelIid")
      .where($"mostPopularChannelIid" === $"channelIid")
      .select("campaignId", "channelIid")
  }

}
