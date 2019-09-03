package juanignaciosl.yelp

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection
import com.twitter.algebird.{Aggregator, Semigroup}
import juanignaciosl.utils.MathUtils
import org.slf4j.LoggerFactory

/*
sbt "runMain [PACKAGE].WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object YelpBusinessesRunner extends YelpDataProcessor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val inputDir = args.getOrElse("input", "/tmp/yelp_data")
    val outputDir = args.getOrElse("output", "/tmp/yelp-scio")

    val businessesPath = s"$inputDir/business.zip"
    val businesses = sc.jsonFile[Business](businessesPath)
    val openBusinesses = filterWithHours(filterOpenBusinesses(businesses))

    val openBusinessesByStateAndCity = openBusinesses.keyBy(b => (b.state, b.city))
    computeOpenBusinesses(outputDir, openBusinessesByStateAndCity)
    computePercentiles(outputDir, openBusinessesByStateAndCity, List(.5, .95))

    computeCoolestBusinesses(outputDir, openBusinesses, sc.jsonFile[Review](s"$inputDir/review.zip"))

    val result = sc.close().waitUntilFinish()
    logger.info(s"Done! State: ${result.state}")
  }

  private def computeOpenBusinesses(outputDir: String,
                                    businessesByStateAndCity: SCollection[((StateAbbr, City), Business)]): Unit = {
    val hoursByStateAndCity = businessesByStateAndCity.flatMapValues(_.hours)
    val openPast2100 = countOpenPastTime(hoursByStateAndCity, "21:00")
    openPast2100.map(openPastToString).saveAsTextFile(s"$outputDir/openpast-2100.csv")
  }

  private def computePercentiles(outputDir: String,
                                 businessesByStateAndCity: SCollection[((StateAbbr, City), Business)],
                                 percentiles: List[Double]): Unit = {
    val businessesByPostalCode = businessesByStateAndCity.keyBy {
      case ((state, city), b) => (state, city, b.postal_code)
    }.mapValues(_._2)

    val hoursByPostalCode = businessesByPostalCode.flatMapValues(_.hours)

    List(("opening", 0), ("closing", 1)).map {
      case (name, splitIndex) => {
        val computedPercentiles = computePercentiles(hoursByPostalCode, percentiles, _.split('-')(splitIndex))
        val formattedOutput = percentiles.zipWithIndex.map {
          case (_, i) => computedPercentiles.map {
            case (grouping, percentileTimesPerDay) => dailyPercentileToString(
              grouping,
              Business.days.map {
                day => percentileTimesPerDay.get(day).map(_ (i))
              })
          }
        }
        formattedOutput.zip(percentiles).map {
          case (values, p) => values.saveAsTextFile(s"$outputDir/$name-$p.csv")
        }
      }
    }
  }

  private def computeCoolestBusinesses(outputDir: String,
                                       businesses: SCollection[Business],
                                       reviews: SCollection[Review]): Unit = {
    val topBusinesses = computeCoolestBusinesses(businesses, reviews)
    topBusinesses.map(topBusinessToString(_)).saveAsTextFile(s"$outputDir/coolestBusinessNotOpenOnSunday.csv")
  }

}

case class Business(business_id: BusinessId,
                    is_open: Int,
                    postal_code: PostalCode,
                    city: City,
                    state: StateAbbr,
                    hours: Option[BusinessWeekSchedule]) extends Serializable {
  lazy val isOpen: Boolean = is_open == 1

  def isOpen(day: WeekDay): Boolean = hours.flatMap(_.get(day)).isDefined
}

object Business {
  def toHHMM(time: BusinessTime): BusinessTime = {
    time.split(':') match {
      case Array(hh, mm) => f"${hh.toInt}%02d:${mm.toInt}%02d"
      // INFO: simplistic approach to error handling; a better approach would involve monads such as Try,
      // at the cost of increased complexity
      case _ => throw new Exception(s"Error parsing $time")
    }
  }

  def contains(time: BusinessSchedule, contained: BusinessSchedule): Boolean = {
    (time.split('-').map(toHHMM), toHHMM(contained)) match {
      // If closing time is before open time it must be because it spans to the next day
      case (Array(o, c), t) => o <= t && (t < c || c < o)
      case _ => false
    }
  }

  val days = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

  def openDays(hours: BusinessWeekSchedule, time: BusinessSchedule): List[Boolean] = {
    days.map(hours.get).map(_.map(contains(_, time))).map(_.getOrElse(false))
  }

}

object PercentileSemigroup extends Semigroup[Map[WeekDay, Vector[BusinessTime]]] {
  override def plus(x: Map[WeekDay, Vector[BusinessTime]],
                    y: Map[WeekDay, Vector[BusinessTime]]): Map[WeekDay, Vector[BusinessTime]] = {
    import cats.implicits._
    x.combine(y)
  }
}

class PercentileAggregator(ps: List[Double], f: BusinessSchedule => BusinessTime) extends
  Aggregator[BusinessWeekSchedule, Map[WeekDay, Vector[BusinessTime]], Map[WeekDay, List[BusinessTime]]]
  with MathUtils {
  override def prepare(input: BusinessWeekSchedule): Map[WeekDay, Vector[BusinessTime]] =
    input.mapValues(s => Vector(Business.toHHMM(f(s))))

  override def semigroup: Semigroup[Map[WeekDay, Vector[BusinessTime]]] = PercentileSemigroup

  override def present(reduction: Map[WeekDay, Vector[BusinessTime]]): Map[WeekDay, List[BusinessTime]] = {
    reduction.mapValues {
      percentile(_, ps)
    }
  }
}

trait YelpDataProcessor {
  def filterOpenBusinesses(businesses: SCollection[Business]): SCollection[Business] = {
    businesses.filter(_.isOpen)
  }

  def filterWithHours(businesses: SCollection[Business]): SCollection[Business] = {
    businesses.filter(_.hours.isDefined)
  }

  def countOpenPastTime(hours: SCollection[((StateAbbr, City), BusinessWeekSchedule)],
                        time: BusinessTime): SCollection[((StateAbbr, City), WeekCount)] = {
    import Business._

    hours.aggregateByKey(List(0, 0, 0, 0, 0, 0, 0))(
      (count, hours) => count.zip(openDays(hours, time))
        .map { case (c, contained) => if (contained) c + 1 else c },
      (count1, count2) => count1.zip(count2).map { case (a, b) => a + b }
    )
  }

  def openPastToString(tuple: ((StateAbbr, City), List[Int])): String = {
    s"${tuple._1._1},${tuple._1._2},${tuple._2.map(_.toString).mkString(",")}"
  }

  def computePercentiles(hoursByPostalCode: SCollection[((StateAbbr, City, PostalCode), BusinessWeekSchedule)],
                         ps: List[Double],
                         f: BusinessSchedule => BusinessTime):
  SCollection[((StateAbbr, City, PostalCode), Map[WeekDay, List[BusinessTime]])] = {
    val aggregator = new PercentileAggregator(ps, f)
    hoursByPostalCode.aggregateByKey(aggregator)
  }

  def dailyPercentileToString(grouping: (StateAbbr, City, PostalCode),
                              percentilePerDay: List[Option[BusinessTime]]): String = {
    s"${grouping._1},${grouping._2},${grouping._3},${percentilePerDay.map(_.getOrElse("")).mkString(",")}"
  }

  def topBusinessToString(tuple: ((StateAbbr, City, PostalCode), (BusinessId, CoolnessCount))): String = {
    s"${tuple._1._1},${tuple._1._2},${tuple._1._3},${tuple._2._1},${tuple._2._2}"
  }

  def computeCoolestBusinesses(businesses: SCollection[Business],
                               reviews: SCollection[Review]):
  SCollection[((StateAbbr, City, PostalCode), (BusinessId, CoolnessCount))] = {
    val notOpenOnSunday = businesses
      .filter(!_.isOpen("Sunday"))
      .keyBy(_.business_id)
      .mapValues(b => (b.state, b.city, b.postal_code))
    val coolnessCount = reviews.keyBy(_.business_id).mapValues(_.cool).sumByKey
    val businessesWithCoolness = coolnessCount.join(notOpenOnSunday)
    val groupedByPostalCode = businessesWithCoolness.keyBy(_._2._2).mapValues(v => (v._1, v._2._1))
    groupedByPostalCode.topByKey(1, Ordering.by(_._2)).mapValues(_.head)
  }

}

case class Review(review_id: ReviewId, business_id: BusinessId, cool: CoolnessCount)
