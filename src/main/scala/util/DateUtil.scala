package util

import java.time.LocalDateTime

object DateUtil {

  def getLocalDateTimeRange(startDate: LocalDateTime, endDate: LocalDateTime): Seq[LocalDateTime] = {
    Stream.iterate(startDate)(_.plusHours(1)).takeWhile(_.isBefore(endDate)).toList
  }
}
