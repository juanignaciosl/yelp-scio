package juanignaciosl.yelp

import com.spotify.scio._
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection
import org.slf4j.LoggerFactory

/*
sbt "runMain [PACKAGE].WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object YelpBusinessesLocalRunner extends YelpDataProcessor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val inputDir = args.getOrElse("input", "/tmp/yelp_data")
    val outputDir = args.getOrElse("output", "/tmp/yelp-scio")

    val businessesPath = s"$inputDir/business.json"
    val businesses = sc.jsonFile[Business](businessesPath)
    val openBusinesses = filterWithHours(filterOpenBusinesses(businesses))

    val hoursByStateAndCity = openBusinesses.keyBy(b => (b.state, b.city)).flatMapValues(_.hours)
    val openPast2100 = countOpenPastTime(hoursByStateAndCity, "21:00")

    openPast2100.map(openPastToString).saveAsTextFile(s"$outputDir/openpast-2100.csv")
    val result = sc.run().waitUntilFinish()
    logger.info(s"Done! State: ${result.state}")
  }

}

case class Business(business_id: BusinessId,
                    is_open: Int,
                    postal_code: PostalCode,
                    city: City,
                    state: StateAbbr,
                    hours: Option[BusinessWeekSchedule]) extends Serializable {
  lazy val isOpen: Boolean = is_open == 1
}

object Business {
  def toHHMM(time: BusinessTime): Option[BusinessTime] = {
    time.split(':') match {
      // INFO: this is a simplistic approach to ease flatmapping with other Option monads
      case Array(hh, mm) => Some(f"${hh.toInt}%02d:${mm.toInt}%02d")
      case _ => None
    }
  }

  def contains(time: BusinessSchedule, contained: BusinessSchedule): Boolean = {
    (time.split('-').map(toHHMM), toHHMM(contained)) match {
      // If closing time is before open time it must be because it spans to the next day
      case (Array(Some(o), Some(c)), Some(t)) => o <= t && (t < c || c < o)
      case _ => false
    }
  }

  val days = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

  def openDays(hours: BusinessWeekSchedule, time: BusinessSchedule): List[Boolean] = {
    days.map(hours.get(_)).map(_.map(contains(_, time))).map(_.getOrElse(false))
  }

}

trait YelpDataProcessor {
  def filterOpenBusinesses(businesses: SCollection[Business]): SCollection[Business] = {
    businesses.filter(_.isOpen)
  }

  def filterWithHours(businesses: SCollection[Business]): SCollection[Business] = {
    businesses.filter(_.hours.isDefined)
  }

  def countOpenPastTime(hours: SCollection[((StateAbbr, City), BusinessWeekSchedule)], time: BusinessTime):
  SCollection[((StateAbbr, City), List[Int])] = {
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

}
