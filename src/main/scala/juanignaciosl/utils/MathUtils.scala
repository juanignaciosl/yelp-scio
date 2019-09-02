package juanignaciosl.utils

trait MathUtils {
  def percentile[T](values: Vector[T], percentiles: List[Double])
                   (implicit ordering: Ordering[T]): List[T] = {
    if (values.isEmpty) {
      List()
    } else {
      val sorted = values.sorted
      percentiles.map { p =>
        val size = sorted.size
        sorted(Math.ceil(p * size).toInt - 1)
      }
    }
  }

}
