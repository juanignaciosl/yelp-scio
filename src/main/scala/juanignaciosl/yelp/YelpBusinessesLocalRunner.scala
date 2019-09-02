package juanignaciosl.yelp

import com.spotify.scio._
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection

/*
sbt "runMain [PACKAGE].WordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
*/

object YelpBusinessesLocalRunner extends YelpDataProcessor {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val inputDir = args.getOrElse("input", "/tmp/yelp_data")
    val outputDir = args.getOrElse("output", "/tmp/yelp-scio")

    val businessesPath = s"$inputDir/business.json"
    val businesses = sc.jsonFile[BusinessLine](businessesPath)
    val openBusinesses = filterWithHours(filterOpenBusinesses(businesses))
    openBusinesses.count.saveAsTextFile(s"$outputDir/xxx.csv")

    sc.close().waitUntilFinish()
  }

}

case class BusinessLine(business_id: BusinessId,
                        is_open: Int,
                        postal_code: PostalCode,
                        city: City,
                        state: StateAbbr,
                        hours: Option[Map[WeekDay, BusinessSchedule]]) extends Serializable {
  lazy val isOpen: Boolean = is_open == 1
}

trait YelpDataProcessor {
  def filterOpenBusinesses(businesses: SCollection[BusinessLine]): SCollection[BusinessLine] = {
    businesses.filter(_.isOpen)
  }

  def filterWithHours(businesses: SCollection[BusinessLine]): SCollection[BusinessLine] = {
    businesses.filter(_.hours.isDefined)
  }
}
