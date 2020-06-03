package util

import java.time.LocalDateTime

import org.scalatest.FunSuite
class DateUtilTest extends FunSuite {
  test("Range of hours between 2 dates") {
    val startDate = LocalDateTime.of(2020, 6, 1, 23, 0)
    val endDate = LocalDateTime.of(2020, 6, 2, 2, 0)
    val result = DateUtil.getHoursRange(startDate, endDate)
    val expected = Seq(
      LocalDateTime.of(2020, 6, 1, 23, 0),
      LocalDateTime.of(2020, 6, 2, 0, 0),
      LocalDateTime.of(2020, 6, 2, 1, 0)
    )
    assertResult(expected)(result)
  }
  test("Range of hours between the same date") {
    val startDate = LocalDateTime.of(2020, 6, 1, 23, 0)
    val endDate = LocalDateTime.of(2020, 6, 1, 23, 0)
    val result = DateUtil.getHoursRange(startDate, endDate)
    val expected = Seq()
    assertResult(expected)(result)
  }
  test("Range of hours with endDate prior to startDate") {
    val startDate = LocalDateTime.of(2020, 6, 2, 2, 0)
    val endDate = LocalDateTime.of(2020, 6, 1, 23, 0)
    val result = DateUtil.getHoursRange(startDate, endDate)
    val expected = Seq()
    assertResult(expected)(result)
  }
}
