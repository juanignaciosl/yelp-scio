package juanignaciosl.yelp

import com.spotify.scio.extra.json.JsonIO
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing._
import io.circe.generic.auto._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class YelpBusinessesLocalRunnerTest extends PipelineSpec {

  private def businessTemplate = Business(
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

  "YelpStatsRunner" should "create openpast file" in {
    val businessesData = Seq(businessTemplate)
    val outputOpenPastFile = s"$outputDir/openpast-2100.csv"
    val outputOpening05 = s"$outputDir/opening-0.5.csv"
    val outputOpening95 = s"$outputDir/opening-0.95.csv"
    val outputClosing05 = s"$outputDir/closing-0.5.csv"
    val outputClosing95 = s"$outputDir/closing-0.95.csv"
    val expectedOpenPast = "My State,My City,1,0,0,0,0,0,0"
    val expectedOpening05 = "My State,My City,Z1PC0D3,09:00,,,,,,"
    val expectedOpening95 = "My State,My City,Z1PC0D3,09:00,,,,,,"
    val expectedClosing05 = "My State,My City,Z1PC0D3,00:00,,,,,,"
    val expectedClosing95 = "My State,My City,Z1PC0D3,00:00,,,,,,"
    JobTest[YelpBusinessesLocalRunner.type]
      .args(s"--input=$dataDir", s"--output=$outputDir")
      .input(JsonIO[Business](inputFile), businessesData)
      .output(TextIO(outputOpenPastFile))(_ should containValue(expectedOpenPast))
      .output(TextIO(outputOpening05))(_ should containValue(expectedOpening05))
      .output(TextIO(outputOpening95))(_ should containValue(expectedOpening95))
      .output(TextIO(outputClosing05))(_ should containValue(expectedClosing05))
      .output(TextIO(outputClosing95))(_ should containValue(expectedClosing95))
      .run()
  }

}

class YelpDataProcessorTest extends PipelineSpec with YelpDataProcessor {
  private def businessTemplate = Business(
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
      openBusinesses shouldNot containValue(closedBusiness)
      openBusinesses should containValue(openBusiness)
    }
  }

  "filterWithHours" should "drop businesses without hours" in {
    val withHours = businessTemplate.copy(hours = Some(Map("Monday" -> "9:0-0:0")))
    val withoutHours = businessTemplate.copy(hours = None)
    runWithContext { sc =>
      val businessesWithHours = filterWithHours(sc.parallelize(Seq(withHours, withoutHours)))
      businessesWithHours shouldNot containValue(withoutHours)
      businessesWithHours should containValue(withHours)
    }
  }

  "countOpenPastTime" should "count businesses open at a given time" in {
    val from09to20No = Map("Monday" -> "9:00-20:00")
    val from09to21No = Map("Monday" -> "9:00-21:00")
    val from09to22Yes = Map("Monday" -> "9:00-22:00")
    val from22to23YNo = Map("Monday" -> "22:00-23:00")
    val hours = Seq(from09to20No, from09to21No, from09to22Yes, from22to23YNo)
      .map(("State", "City", _))
    runWithContext { sc =>
      val groupedHours = sc.parallelize(hours).keyBy(h => (h._1, h._2)).mapValues(_._3)
      val openBusinessesCount = countOpenPastTime(groupedHours, "21:00")

      val expected = (("State", "City"), List(1, 0, 0, 0, 0, 0, 0))
      openBusinessesCount should containSingleValue(expected)
    }
  }

  "computePercentiles" should "compute .5 and .95 percentiles when there's only one matching business" in {
    val hours = Seq(
      Map("Monday" -> "9:00-10:00"),
      Map("Tuesday" -> "10:00-11:00")
    ).map(("State", "City", "PostalCode", _))
    runWithContext { sc =>
      val groupedHours = sc.parallelize(hours).keyBy(h => (h._1, h._2, h._3)).mapValues(_._4)

      val result = computePercentiles(groupedHours, List(.5, .95), _.split('-')(0))
      val expected = (("State", "City", "PostalCode"),
        Map(
          "Monday" -> List("09:00", "09:00"),
          "Tuesday" -> List("10:00", "10:00")
        )
      )
      result should containSingleValue(expected)
    }
  }

  "computePercentiles" should "compute .5 and .95 percentiles" in {
    val hours = (8 to 11).map(h =>
      Map(
        "Monday" -> s"$h:0-0:0",
        "Tuesday" -> s"${h + 1}:0-0:0"
      )
    ).map(("State", "City", "PostalCode", _))
    runWithContext { sc =>
      val groupedHours = sc.parallelize(hours).keyBy(h => (h._1, h._2, h._3)).mapValues(_._4)

      val result = computePercentiles(groupedHours, List(.5, .95), _.split('-')(0))
      val expected = (("State", "City", "PostalCode"),
        Map(
          "Monday" -> List("09:00", "11:00"),
          "Tuesday" -> List("10:00", "12:00")
        )
      )
      result should containSingleValue(expected)
    }
  }
}

class BusinessSpec extends FlatSpec with Matchers {

  import Business._

  "openDays" should "return if days are open at a given time" in {
    val hours = Map(
      "Monday" -> "9:0-20:00",
      "Tuesday" -> "9:0-20:00",
      "Wednesday" -> "9:0-20:00",
      "Thursday" -> "9:0-20:00",
      "Friday" -> "9:0-20:00",
      "Saturday" -> "9:0-22:00",
      "Sunday" -> "9:0-22:00"
    )

    openDays(hours, "21:00") should equal(List(false, false, false, false, false, true, true))
  }

  "contains" should "say if a schedule contains a given time" in {
    contains("9:0-11:00", "8:00") should equal(false)
    contains("9:0-11:00", "9:00") should equal(true)
    contains("9:0-11:00", "11:00") should equal(false)
    contains("9:0-11:00", "12:00") should equal(false)
  }
}
