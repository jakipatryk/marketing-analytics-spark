import com.utils.WeeklyPurchasesProjection
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class WeeklyPurchasesProjectionSpec extends AnyFlatSpec with FakeDataLoader {

  implicit val spark: SparkSession = SparkSession
    .builder().master("local[*]")
    .appName("PurchasesAttributionProjectionSpec")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  "create" should "create a summary of purchases grouped by quarter and week of the year" in {
    val (_, purchases) = loadFakeData()
    val df = WeeklyPurchasesProjection.create(purchases)
    val result = df.collect()

    assert(result.length == 2)
    assert(result contains Row("3", 33, 3, 3719.54))
    assert(result contains Row("3", 34, 1, 21.37))
  }

}
