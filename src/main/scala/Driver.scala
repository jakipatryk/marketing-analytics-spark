import com.utils.{CampaignsAndChannelsStatistics, DataHandler, EventsToEventsWithSession, PurchasesAttributionProjection}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, to_date}

object Driver extends App with DataHandler {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("GridUMarketingAnalytics")
    .getOrCreate()

  import spark.implicits._

  doTask(`task #1.1`)

  spark.stop()

  def doTask(task: () => Unit, measureTime: Boolean = false): Unit = {
    if(measureTime) spark.time(task())
    else task()
  }

  def `task #1.1`(): Unit = {
    val (events, purchases) = inputFromCsv
    val purchasesAttribution = PurchasesAttributionProjection.viaPlainSparkSQL(events, purchases)
//    purchasesAttribution.write.parquet("src/main/resources/out/1_1/")
    purchasesAttribution
      .withColumn("purchaseTimeDate", to_date($"purchaseTime", "yyyy-mm-dd"))
      .write
      .partitionBy("purchaseTimeDate")
      .parquet("src/main/resources/out/1_1")
  }

  def `task #1.2`(): Unit = {
    val (events, purchases) = inputFromCsv
    val purchasesAttribution = PurchasesAttributionProjection.viaAggregator(events, purchases)
    purchasesAttribution.write.parquet("src/main/resources/out/1_2/")
  }

  def `task #2.1`(): Unit = {
    val purchasesAttribution = spark.read.parquet("src/main/resources/out/1_1")
    val topRevenue = CampaignsAndChannelsStatistics.topCampaignsByRevenue(purchasesAttribution)
    topRevenue.write.parquet("src/main/resources/out/2_1")
  }

  def `task #2.2`(): Unit = {
    val (events, _) = inputFromCsv
    val evensWithSession = EventsToEventsWithSession.convert(events)
    val topChannelsPerCampaign = CampaignsAndChannelsStatistics.mostPopularChannelsInCampaigns(evensWithSession)
    topChannelsPerCampaign.write.parquet("src/main/resources/out/2_2")
  }

  def `task #3.2`(): Unit = {
    val (events, purchases) = inputFromParquetPartitionedByDate

    val inSeptemberEvents = events
      .where(
        $"eventTimeDate".geq(lit("2020-10-01"))
          && $"eventTimeDate".leq(lit("2020-10-31"))
      )
    val inSeptemberPurchases = purchases
      .where(
        $"purchaseTimeDate".geq(lit("2020-10-01"))
          && $"purchaseTimeDate".leq(lit("2020-10-31"))
      )
    val inSeptemberPurchasesAttribution = PurchasesAttributionProjection
      .viaPlainSparkSQL(inSeptemberEvents, inSeptemberPurchases)
    val topCampaignsInSeptember = CampaignsAndChannelsStatistics.topCampaignsByRevenue(inSeptemberPurchasesAttribution)
    topCampaignsInSeptember
      .write
      .parquet("src/main/resources/out/3_2/september/top_campaigns")
    val topChannelsInCampaignsInSeptember = CampaignsAndChannelsStatistics
        .mostPopularChannelsInCampaigns(EventsToEventsWithSession.convert(inSeptemberEvents))
    topChannelsInCampaignsInSeptember
      .write
      .parquet("src/main/resources/out/3_2/september/most_popular_channels")

    val atSpecificDayEvents = events.where($"eventTimeDate" === lit("2020-11-11"))
    val atSpecificDayPurchases = purchases.where($"purchaseTimeDate" === lit("2020-11-11"))
    val atSpecificDayPurchaseAttributes = PurchasesAttributionProjection
      .viaPlainSparkSQL(atSpecificDayEvents, atSpecificDayPurchases)
    val topCampaignsAtSpecificDay = CampaignsAndChannelsStatistics
      .topCampaignsByRevenue(atSpecificDayPurchaseAttributes)
    topCampaignsAtSpecificDay
      .write
      .parquet("src/main/resources/out/3_2/2020-11-11/top_campaigns")
    val topChannelsInCampaignsAtSpecificDay = CampaignsAndChannelsStatistics
      .mostPopularChannelsInCampaigns(EventsToEventsWithSession.convert(atSpecificDayEvents))
    topChannelsInCampaignsAtSpecificDay
      .write
      .parquet("src/main/resources/out/3_2/2020-11-11/most_popular_channels")
  }

}
