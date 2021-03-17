package com.utils

import org.apache.spark.sql.functions.sum
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

  def topCampaignsByRevenueWithoutSQL(
                                       purchasesAttribution: DataFrame, n: Int = 10
                                     )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    purchasesAttribution
      .where($"isConfirmed" === true)
      .groupBy($"campaignId")
      .agg(sum($"billingCost") as "revenue")
      .orderBy($"revenue".desc)
      .limit(n)
  }

}
