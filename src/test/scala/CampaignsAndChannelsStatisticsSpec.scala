import com.utils.{CampaignsAndChannelsStatistics, PurchasesAttributionProjection}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class CampaignsAndChannelsStatisticsSpec extends AnyFlatSpec with FakeDataLoader {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("PurchasesAttributionProjectionSpec")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  "topCampaignsByRevenue" should "return DF with top n campaigns by revenue" in {
    val (events, purchases) = loadFakeData()
    val purchasesAttribution = PurchasesAttributionProjection.viaPlainSparkSQL(events, purchases)
    val top2Campaigns = CampaignsAndChannelsStatistics.topCampaignsByRevenue(purchasesAttribution, 2).collect()

    assert(top2Campaigns.length == 2)
    assert(top2Campaigns(0) == Row("c1", 719.54))
    assert(top2Campaigns(1) == Row("c2", 21.37))
  }

  "topCampaignsByRevenueWithoutSQL" should "return equivalent result to version with SQL" in {
    val (events, purchases) = loadFakeData()
    val purchasesAttribution = PurchasesAttributionProjection.viaPlainSparkSQL(events, purchases)
    val top2CampaignsSQL = CampaignsAndChannelsStatistics.topCampaignsByRevenue(purchasesAttribution, 2).collect()
    val top2CampaignsWithoutSQL = CampaignsAndChannelsStatistics
      .topCampaignsByRevenueWithoutSQL(purchasesAttribution, 2)
      .collect()

    assert(top2CampaignsSQL.sameElements(top2CampaignsWithoutSQL))
  }

}
