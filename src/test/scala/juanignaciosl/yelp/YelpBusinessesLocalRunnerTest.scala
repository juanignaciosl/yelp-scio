package juanignaciosl.yelp

import com.spotify.scio.extra.json.JsonIO
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing._
import io.circe.generic.auto._

import scala.util.Random

class YelpBusinessesLocalRunnerTest extends PipelineSpec {

  private def businessTemplate = BusinessLine(
    Random.nextString(4),
    1,
    "Z1PC0D3",
    "My City",
    "My State",
    Some(Map("Monday" -> "9:0-0:0"))
  )

  val dataDir = "/tmp/yelp_data"
  val inputFile = s"$dataDir/business.json"
  val outputDir = "/tmp/yelp-scio-test"

  "YelpStatsRunner" should "count" in {
    val businessesData = Seq(businessTemplate)
    val outputFile = s"$outputDir/xxx.csv"
    val expected = Seq(businessesData.length.toString)
    JobTest[YelpBusinessesLocalRunner.type]
      .args(s"--input=$dataDir", s"--output=$outputDir")
      .input(JsonIO[BusinessLine](inputFile), businessesData)
      .output(TextIO(outputFile))(_ should containInAnyOrder(expected))
      .run()
  }

}

class YelpDataProcesorTest extends PipelineSpec with YelpDataProcessor {
  private def businessTemplate = BusinessLine(
    Random.nextString(4),
    1,
    "Z1PC0D3",
    "My City",
    "My State",
    Some(Map("Monday" -> "9:0-0:0"))
  )

  "filterOpenBusinesses" should "drop closed businesses" in {
    val openBusiness = businessTemplate.copy(is_open = 1)
    val closedBusiness = businessTemplate.copy(is_open = 0)
    val businesses = Seq(openBusiness, closedBusiness)
    runWithContext { sc =>
      val openBusinesses = filterOpenBusinesses(sc.parallelize(businesses))
      openBusinesses shouldNot containInAnyOrder(Seq(closedBusiness))
      openBusinesses should containInAnyOrder(Seq(openBusiness))
    }
  }
}
