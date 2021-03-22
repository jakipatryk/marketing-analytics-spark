package com.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, date_format, sum, floor}

object WeeklyPurchasesProjection {
  def create(purchases: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    purchases
      .withColumn("quarter", date_format($"purchaseTime", "q"))
      .withColumn("week", floor(date_format($"purchaseTime", "D") / 7))
      .groupBy("quarter", "week")
      .agg(
        count($"billingCost") as "weeklyTotalPurchases",
        sum($"billingCost") as "weeklyTotalBillingCost"
      )
  }

}
