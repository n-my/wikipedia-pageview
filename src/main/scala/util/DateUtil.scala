package util

import java.time.LocalDateTime

/**
 * Helpers to manipulate [[java.time.LocalDateTime]]
 */
object DateUtil {

  /**
   *
   * Get a range of [[LocalDateTime]], incremented by 1h, between 2 [[LocalDateTime]]
   *
   * @param startDate the start date, included
   * @param endDate the end date, excluded
   * @return the range of dates between `startDate` and `endDate`
   */
  def getHoursRange(startDate: LocalDateTime, endDate: LocalDateTime): Seq[LocalDateTime] = {
    Stream
      .iterate(startDate)(_.plusHours(1))
      .takeWhile(_.isBefore(endDate))
  }
}
