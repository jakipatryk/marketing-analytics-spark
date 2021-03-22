import com.utils.{CampaignsAndChannelsStatistics, Config, DataLoader, PurchasesAttributionProjection, WeeklyPurchasesProjection}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{lit, to_date}

object Driver extends App with DataLoader {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
//    .config("spark.sql.adaptive.enabled", "true")
    .appName("GridUMarketingAnalytics")
    .getOrCreate()

  import spark.implicits._

  val mode: String = args.head
  implicit val config: Config = Config.parseFromCommandLine(args.tail)
  val measureTime = true

  doTask(mode match {
    case "prepare_parquet_inputs" => prepareParquetInputs
    case "task_1_1" => `task #1.1`
    case "task_1_2" => `task #1.2`
    case "task_2_1" => `task #2.1`
    case "task_2_2" => `task #2.2`
    case "task_3_2_september" => {
      val startDate = "2020-10-01"
      val endDate = "2020-10-31"
      `task #3.2`(
        eventsBetweenDatesCondition(startDate, endDate),
        purchasesBetweenDatesCondition(startDate, endDate),
        "september"
      )
    }
    case "task_3_2_2020_11_11" => {
      val date = "2020-11-11"
      `task #3.2`(eventsExactDateCondition(date), purchasesExactDateCondition(date), date)
    }
    case "task_3_3" => `task #3.3`
  }, measureTime)

  spark.stop()

  def doTask(task: () => Unit, measureTime: Boolean = false): Unit = {
    if(measureTime) spark.time(task())
    else task()
  }

  def `task #1.1`(): Unit = {
    val (events, purchases) = loadInput
    val purchasesAttribution = PurchasesAttributionProjection.viaPlainSparkSQL(events, purchases)
    purchasesAttribution
      .write
      .mode("overwrite")
      .parquet(s"${config.outputBasePath}/1_1")
  }

  def `task #1.2`(): Unit = {
    val (events, purchases) = loadInput
    val purchasesAttribution = PurchasesAttributionProjection.viaAggregator(events, purchases)
    purchasesAttribution.write.mode("overwrite").parquet(s"${config.outputBasePath}1_2/")
  }

  def `task #2.1`(): Unit = {
    val (events, purchases) = loadInput
    val purchasesAttribution = PurchasesAttributionProjection.viaAggregator(events, purchases)
    val topRevenue = CampaignsAndChannelsStatistics.topCampaignsByRevenue(purchasesAttribution)
    topRevenue.write.mode("overwrite").parquet(s"${config.outputBasePath}2_1")
  }

  def `task #2.2`(): Unit = {
    val (events, _) = loadInput
    val topChannelsPerCampaign = CampaignsAndChannelsStatistics.mostPopularChannelsInCampaigns(events)
    topChannelsPerCampaign.write.mode("overwrite").parquet(s"${config.outputBasePath}2_2")
  }

  def `task #3.2`(eventsDateCondition: Column, purchasesDateCondition: Column, pathName: String)(): Unit = {
    val (events, purchases) = loadInput

    val filteredEvents = {
      if(events.schema.fieldNames contains "eventTimeDate") events
      else events.withColumn("eventTimeDate", to_date($"eventTime", "yyyy-mm-dd"))
    }.where(eventsDateCondition)
    val filteredPurchases = {
      if(purchases.schema.fieldNames contains "purchaseTimeDate") purchases
      else purchases.withColumn("purchaseTimeDate", to_date($"purchaseTime", "yyyy-mm-dd"))
    }.where(purchasesDateCondition)

    val purchasesAttribution = PurchasesAttributionProjection.viaPlainSparkSQL(filteredEvents, filteredPurchases)
    val topCampaigns = CampaignsAndChannelsStatistics.topCampaignsByRevenue(purchasesAttribution)
    topCampaigns
      .write
      .mode("overwrite")
      .parquet(s"${config.outputBasePath}$pathName/top_campaigns")

    val topChannelsInCampaigns = CampaignsAndChannelsStatistics.mostPopularChannelsInCampaigns(filteredEvents)
    topChannelsInCampaigns
      .write
      .mode("overwrite")
      .parquet(s"${config.outputBasePath}$pathName/most_popular_channels")
  }

  def `task #3.3`(): Unit = {
    val (_, purchases) = loadInput
    val weeklyPurchases = WeeklyPurchasesProjection.create(purchases)
    weeklyPurchases.write.mode("overwrite").parquet(s"${config.outputBasePath}3_3")
  }

  private def eventsBetweenDatesCondition(start: String, end: String): Column =
    $"eventTimeDate".geq(lit(start)) && $"eventTimeDate".leq(lit(end))

  private def eventsExactDateCondition(date: String): Column =
    $"eventTimeDate" === lit(date)

  private def purchasesBetweenDatesCondition(start: String, end: String): Column =
    $"purchaseTimeDate".geq(lit(start)) && $"purchaseTimeDate".leq(lit(end))

  private def purchasesExactDateCondition(date: String): Column =
    $"purchaseTimeDate" === lit(date)

  private def prepareParquetInputs(): Unit = {
    val (events, purchases) = loadInput

    events.write.parquet(config.outputBasePath + "no_partitioning/events/")
    purchases.write.parquet(config.outputBasePath + "no_partitioning/purchases")

    events
      .write
      .partitionBy("eventType")
      .parquet(config.outputBasePath + "partition_by_eventType-isConfirmed/events/")
    purchases
      .write
      .partitionBy("isConfirmed")
      .parquet(config.outputBasePath + "partition_by_eventType-isConfirmed/purchases/")

    events
      .withColumn("eventTimeDate", to_date($"eventTime", "yyyy-mm-dd"))
      .write
      .partitionBy("eventTimeDate")
      .parquet(config.outputBasePath + "partition_by_date/events/")
    purchases
      .withColumn("purchaseTimeDate", to_date($"purchaseTime", "yyyy-mm-dd"))
      .write
      .partitionBy("purchaseTimeDate")
      .parquet(config.outputBasePath + "partition_by_date/purchases/")
  }

}
