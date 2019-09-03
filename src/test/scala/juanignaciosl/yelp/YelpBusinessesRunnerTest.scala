package juanignaciosl.yelp

import com.spotify.scio.extra.json.JsonIO
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing._
import io.circe.generic.auto._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class YelpBusinessesRunnerTest extends PipelineSpec {

  private def businessTemplate = Business(
    Random.nextString(4),
    1,
    "Z1PC0D3",
    "My City",
    "My State",
    Some(Map("Monday" -> "9:0-0:0"))
  )

  private def reviewTemplate = Review(
    Random.nextString(4),
    Random.nextString(4),
    1
  )

  val dataDir = "/tmp/yelp_data"
  val inputBusinesses = s"$dataDir/business.zip"
  val inputReviews = s"$dataDir/review.zip"
  val outputDir = "/tmp/yelp-scio-test"

  "YelpStatsRunner" should "create openpast file" in {
    val businessesData = Seq(businessTemplate)
    val reviewsData = Seq(reviewTemplate.copy(business_id = businessesData.head.business_id))
    val outputOpenPastFile = s"$outputDir/openpast-2100.csv"
    val outputOpening05 = s"$outputDir/opening-0.5.csv"
    val outputOpening95 = s"$outputDir/opening-0.95.csv"
    val outputClosing05 = s"$outputDir/closing-0.5.csv"
    val outputClosing95 = s"$outputDir/closing-0.95.csv"
    val outputCoolest = s"$outputDir/coolestBusinessNotOpenOnSunday.csv"
    val expectedOpenPast = "My State,My City,1,0,0,0,0,0,0"
    val expectedOpening05 = "My State,My City,Z1PC0D3,09:00,,,,,,"
    val expectedOpening95 = "My State,My City,Z1PC0D3,09:00,,,,,,"
    val expectedClosing05 = "My State,My City,Z1PC0D3,00:00,,,,,,"
    val expectedClosing95 = "My State,My City,Z1PC0D3,00:00,,,,,,"
    val expectedCoolest = s"My State,My City,Z1PC0D3,${businessesData.head.business_id},${reviewsData.head.cool}"
    JobTest[YelpBusinessesRunner.type]
      .args(s"--input=$dataDir", s"--output=$outputDir")
      .input(JsonIO[Business](inputBusinesses), businessesData)
      .input(JsonIO[Review](inputReviews), reviewsData)
      .output(TextIO(outputOpenPastFile))(_ should containValue(expectedOpenPast))
      .output(TextIO(outputOpening05))(_ should containValue(expectedOpening05))
      .output(TextIO(outputOpening95))(_ should containValue(expectedOpening95))
      .output(TextIO(outputClosing05))(_ should containValue(expectedClosing05))
      .output(TextIO(outputClosing95))(_ should containValue(expectedClosing95))
      .output(TextIO(outputCoolest))(_ should containValue(expectedCoolest))
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

  private def reviewTemplate = Review(
    Random.nextString(4),
    Random.nextString(4),
    1
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

  "computeCoolestBusiness" should "compute coolest business" in {
    // 4 groups with businesses (3, 2, 1, 1)
    val b111a = businessTemplate.copy(business_id = "111a", state = "s1", city = "c1", postal_code = "pc1")
    val b111b = businessTemplate.copy(business_id = "111b", state = "s1", city = "c1", postal_code = "pc1")
    val b111c = businessTemplate.copy(business_id = "111c", state = "s1", city = "c1", postal_code = "pc1")

    val b112a = businessTemplate.copy(business_id = "112a", state = "s1", city = "c1", postal_code = "pc2")
    val b112b = businessTemplate.copy(business_id = "112b", state = "s1", city = "c1", postal_code = "pc2")

    val b113a = businessTemplate.copy(business_id = "113a", state = "s1", city = "c1", postal_code = "pc3")

    val b114a = businessTemplate.copy(business_id = "114a", state = "s1", city = "c1", postal_code = "pc4")

    // In groups 1 and 2 the 2nd will win, in 3 the first one, in 4, none
    val r1b111a = reviewTemplate.copy(business_id = b111a.business_id)
    val r1b111b = reviewTemplate.copy(business_id = b111b.business_id)
    val r2b111b = reviewTemplate.copy(business_id = b111b.business_id)

    val r1b112b = reviewTemplate.copy(business_id = b112b.business_id)

    val r1b113a = reviewTemplate.copy(business_id = b113a.business_id)

    runWithContext { sc =>
      val businesses = sc.parallelize(Seq(
        b111a, b111b, b111c, b112a, b112b, b113a, b114a
      ))

      val reviews = sc.parallelize(Seq(
        r1b111a, r1b111b, r2b111b, r1b112b, r1b113a
      ))

      val result = computeCoolestBusinesses(businesses, reviews).values

      val expected = Array(
        (b111b.business_id, 2),
        (b112b.business_id, 1),
        (b113a.business_id, 1)
      )
      result.count should containSingleValue(expected.length.toLong)
      expected.foreach { e =>
        result should containValue(e)
      }
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

  private def businessTemplate = Business(
    Random.nextString(4),
    1,
    "Z1PC0D3",
    "My City",
    "My State",
    None
  )

  "isOpen" should "return true only if it's open that day" in {
    val business = businessTemplate.copy(hours = Some(Map("Monday" -> "9:0-0:0")))
    business.isOpen("Monday") should equal(true)
    business.isOpen("Tuesday") should equal(false)
  }
}
