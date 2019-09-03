package juanignaciosl

package object yelp {

  type BusinessId = String
  type PostalCode = String
  type City = String
  type StateAbbr = String
  type WeekDay = String
  type BusinessSchedule = String
  type BusinessTime = String
  type BusinessWeekSchedule = Map[WeekDay, BusinessSchedule]

  type WeekCount = List[Int]
  type WeekBusinessTime = Map[WeekDay, BusinessTime]

  type ReviewId = String
  type CoolnessCount = Int
}
