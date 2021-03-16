import com.utils.PurchasesAttributionProjection
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class PurchasesAttributionProjectionSpec extends AnyFlatSpec with FakeDataLoader {

  implicit val spark: SparkSession = SparkSession
    .builder().master("local[*]")
    .appName("PurchasesAttributionProjectionSpec")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  "viaPlainSparkSQL" should "contain only expected records" in {
    val (events, purchases) = loadFakeData()
    val df = PurchasesAttributionProjection.viaPlainSparkSQL(events, purchases)
    val result = df.collect()
    assert(result contains Row("p1", "2020-08-19 02:59:48", 687.27, true, "2", "c1", "ch1"))
    assert(result contains Row("p2", "2020-08-19 02:58:48", 32.27, true, "2", "c1", "ch1"))
    assert(result contains Row("p3", "2020-08-19 02:59:58", 3000.00, false, "2", "c1", "ch1"))
    assert(result contains Row("p4", "2020-08-25 08:10:00", 21.37, true, "4", "c2", "ch1"))
    assert(result.length == 4)
  }

}
