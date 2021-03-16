import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait FakeDataLoader {

  def loadFakeData()(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._

    val events: DataFrame = (
      ("u1", "1", "search_product", "2020-08-19 02:44:19", None)
        :: ("u1", "2", "app_open", "2020-08-19 02:43:00", Some(Map("campaign_id" -> "c1", "channel_id" -> "ch1")))
        :: ("u1", "3", "purchase", "2020-08-19 02:59:48", Some(Map("purchase_id" -> "p1")))
        :: ("u2", "4", "app_open", "2020-08-25 08:00:00", Some(Map("campaign_id" -> "c2", "channel_id" -> "ch1")))
        :: ("u1", "5", "purchase", "2020-08-19 02:58:48", Some(Map("purchase_id" -> "p2")))
        :: ("u1", "6", "purchase", "2020-08-19 02:58:58", Some(Map("purchase_id" -> "p3")))
        :: ("u1", "7", "app_close", "2020-08-19 02:58:59", None)
        :: ("u2", "8", "purchase", "2020-08-25 08:10:00", Some(Map("purchase_id" -> "p4")))
        :: ("u2", "9", "app_close", "2020-08-25 08:15:00", None)
        :: ("u3", "10", "app_open", "2020-08-26 08:15:00", Some(Map("campaign_id" -> "c1", "channel_id" -> "ch2")))
        :: ("u3", "11", "app_close", "2020-08-26 08:16:00", None)
        :: Nil
      ).toDF("userId", "eventId", "eventType", "eventTime", "attributes")
      .withColumn("eventTime", $"eventTime".cast(TimestampType))

    val purchases: DataFrame = (
      ("p1", "2020-08-19 02:59:48", 687.27, true)
        :: ("p2", "2020-08-19 02:58:48", 32.27, true)
        :: ("p3", "2020-08-19 02:59:58", 3000.00, false)
        :: ("p4", "2020-08-25 08:10:00", 21.37, true)
        :: Nil
    ).toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed")

    (events, purchases)
  }

}
