package com.utils

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{countDistinct, first, sum}
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

  def mostPopularChannelsInCampaigns(eventsWithSession: DataFrame)(implicit spark: SparkSession): DataFrame = {
    eventsWithSession.createOrReplaceTempView("eventsWithSession")

    val query =
      """SELECT DISTINCT campaignId,
        | FIRST(channelIid) OVER (PARTITION BY campaignId ORDER BY COUNT(DISTINCT sessionId) DESC) as channelIiD
        |FROM eventsWithSession
        |GROUP BY campaignId, channelIiD
        |""".stripMargin

    spark.sql(query)
  }

  def mostPopularChannelsInCampaignsWithoutSQL(eventsWithSession: DataFrame)
                                              (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowSpec = Window
      .partitionBy($"campaignId")
      .orderBy(countDistinct($"sessionId").desc)

    eventsWithSession
      .groupBy("campaignId", "channelIid")
      .agg(first($"channelIid").over(windowSpec) as "mostPopularChannelIid")
      .where($"channelIid" === $"mostPopularChannelIid")
      .select("campaignId", "channelIid")
  }

}
